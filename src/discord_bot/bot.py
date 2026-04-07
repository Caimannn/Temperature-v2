"""Discord control surface for the weather bot."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from datetime import datetime
from datetime import time as dtime
from datetime import timedelta
from datetime import timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DISCORD_IMPORT_ERROR: str | None = None
try:
    import discord
    from discord import app_commands
    from discord.ext import commands, tasks
except ModuleNotFoundError as exc:  # pragma: no cover - import guard for scheduler diagnostics
    discord = None  # type: ignore[assignment]
    app_commands = None  # type: ignore[assignment]
    commands = None  # type: ignore[assignment]
    tasks = None  # type: ignore[assignment]
    DISCORD_IMPORT_ERROR = str(exc)


CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "config.yaml"
POLICY_SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "test_signal_policy.py"
AUDIT_LOG_PATH = Path(__file__).resolve().parents[2] / "output" / "audit" / "live_signal_runs.csv"
AVAILABLE_MODES = ("manual-only", "auto+manual")
DEFAULT_SIGNAL_CITIES = ("nyc", "atlanta", "chicago", "dallas")
DAILY_TRIGGER_HOUR_UTC = 12
DAILY_TRIGGER_MINUTE_UTC = 0


def _utc_now() -> datetime:
    """Return current timezone-aware UTC timestamp."""

    return datetime.now(timezone.utc)


def _next_daily_trigger_utc(now_utc: datetime | None = None) -> datetime:
    """Return the next planned daily trigger timestamp in UTC."""

    now = now_utc or _utc_now()
    today_trigger = now.replace(
        hour=DAILY_TRIGGER_HOUR_UTC,
        minute=DAILY_TRIGGER_MINUTE_UTC,
        second=0,
        microsecond=0,
    )
    if now < today_trigger:
        return today_trigger
    return today_trigger + timedelta(days=1)


@dataclass
class DryRunCityBlock:
    """One parsed dry-run output block for one city."""

    city: str
    discord_preview: str
    source_candidate_count: int
    primary_candidate: str


@dataclass
class DryRunSummary:
    """Parsed summary footer from multi-city dry-run output."""

    total_cities: int
    cities_with_signal: int
    top_signal_city: str
    top_signal_candidate: str


@dataclass
class PanelState:
    """In-memory panel state for the Discord control surface."""

    cities: list[str]
    horizons: list[str]
    active_city_index: int = 0
    active_horizon_index: int = 0
    mode: str = "manual-only"
    kill_switch: bool = False

    @property
    def active_city(self) -> str:
        """Return the current city key."""

        return self.cities[self.active_city_index]

    @property
    def active_horizon(self) -> str:
        """Return the current horizon."""

        return self.horizons[self.active_horizon_index]


def load_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    """Load the JSON-compatible config file used by the bot."""

    return json.loads(path.read_text(encoding="utf-8"))


def build_panel_text(state: PanelState, config: dict[str, Any]) -> str:
    """Render the text-only control panel."""

    city_block = config.get("cities", {}).get(state.active_city, {})
    resolver_type = city_block.get("resolver", {}).get("type", "placeholder")
    mode_label = "AUTO+MANUAL" if state.mode == "auto+manual" else "MANUAL-ONLY"
    kill_label = "ON" if state.kill_switch else "OFF"
    lines = [
        "Weather Bot Control Panel",
        f"City: {state.active_city.upper()} | Horizon: {state.active_horizon.upper()}",
        f"Mode: {mode_label} | Kill Switch: {kill_label}",
        f"Resolver: {resolver_type}",
        "Actions: HOLD | CLOSE | ADD | BUY NEW | SWITCH",
    ]
    return "\n".join(lines)


def build_panel_embed(state: PanelState, config: dict[str, Any], status_text: str | None = None) -> "discord.Embed":
    """Render the panel as a single compact Discord embed."""

    city_block = config.get("cities", {}).get(state.active_city, {})
    resolver_type = city_block.get("resolver", {}).get("type", "placeholder")
    mode_label = "AUTO+MANUAL" if state.mode == "auto+manual" else "MANUAL-ONLY"
    kill_label = "ON" if state.kill_switch else "OFF"
    status_label = status_text or "Ready"

    embed = discord.Embed(title="Weather Bot Control Panel", color=0x2B6CB0)
    embed.add_field(name="City", value=state.active_city.upper(), inline=True)
    embed.add_field(name="Horizon", value=state.active_horizon.upper(), inline=True)
    embed.add_field(name="Mode", value=mode_label, inline=True)
    embed.add_field(name="Kill Switch", value=kill_label, inline=True)
    embed.add_field(name="Resolver", value=resolver_type, inline=True)
    embed.add_field(name="Status", value=status_label, inline=True)
    embed.add_field(name="Actions", value="HOLD | CLOSE | ADD | BUY NEW | SWITCH", inline=False)
    return embed


def build_status_text(kind: str, state: PanelState) -> str:
    """Return placeholder text-only status responses."""

    if kind == "positions":
        return f"Positions: review {state.active_city.upper()} ({state.active_horizon})."
    if kind == "health":
        return "Health: bot connected | providers pending | execution disabled."
    return "Ready."


def _log_panel_event(event: str, detail: str = "") -> None:
    """Minimal stdout logging for panel actions and posting flow."""

    suffix = f" | {detail}" if detail else ""
    print(f"[panel] {event}{suffix}")


def _extract_signal_state_score(preview: str) -> tuple[str, str]:
    """Extract compact state and score from signal preview text."""

    state_match = re.search(r"\b(TRADE_CANDIDATE|PAPER|WATCH|IGNORE)\b", preview)
    state = state_match.group(1) if state_match else "UNKNOWN"
    score_match = re.search(r"score\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", preview, flags=re.IGNORECASE)
    score = score_match.group(1) if score_match else "n/a"
    return state, score


def _append_signal_audit_row(city: str, state: str, candidate: str, score: str, posted: bool) -> None:
    """Append one compact audit row for live signal posting runs."""

    AUDIT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    header = "timestamp,city,state,candidate,score,posted\n"
    row = f"{timestamp},{city},{state},{candidate},{score},{'yes' if posted else 'no'}\n"
    if not AUDIT_LOG_PATH.exists():
        AUDIT_LOG_PATH.write_text(header + row, encoding="utf-8")
        return
    with AUDIT_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(row)


def build_signal_embed(block: DryRunCityBlock) -> "discord.Embed":
    """Build a compact embed for one city signal from dry-run preview text."""

    preview = block.discord_preview
    target_date_match = re.search(r"\b\d{4}-\d{2}-\d{2}\b", preview)
    target_date = target_date_match.group(0) if target_date_match else "today"

    state_match = re.search(r"\b(TRADE_CANDIDATE|PAPER|WATCH|IGNORE)\b", preview)
    state = state_match.group(1) if state_match else "UNKNOWN"

    score_match = re.search(r"score\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", preview, flags=re.IGNORECASE)
    score = score_match.group(1) if score_match else "n/a"

    side = "n/a"
    if "/" in block.primary_candidate:
        side = block.primary_candidate.split("/", 1)[1].replace("_", " ")

    entry_match = re.search(r"entry\s*[:=]\s*([^|]+)", preview, flags=re.IGNORECASE)
    entry = entry_match.group(1).strip() if entry_match else block.primary_candidate

    edge_match = re.search(
        r"(?:executable_edge|exec_edge|edge)\s*[:=]\s*([+-]?[0-9]+(?:\.[0-9]+)?%?)",
        preview,
        flags=re.IGNORECASE,
    )
    executable_edge = edge_match.group(1) if edge_match else "n/a"

    reason_match = re.search(r"reason\s*[:=]\s*([^|]+)", preview, flags=re.IGNORECASE)
    reason = reason_match.group(1).strip() if reason_match else "n/a"

    embed = discord.Embed(title=f"Signal | {block.city.upper()}", color=0x1F8B4C)
    embed.add_field(name="city", value=block.city.upper(), inline=True)
    embed.add_field(name="target_date", value=target_date, inline=True)
    embed.add_field(name="candidate", value=block.primary_candidate, inline=True)
    embed.add_field(name="state", value=state, inline=True)
    embed.add_field(name="score", value=score, inline=True)
    embed.add_field(name="side", value=side, inline=True)
    embed.add_field(name="entry", value=entry, inline=True)
    embed.add_field(name="executable_edge", value=executable_edge, inline=True)
    embed.add_field(name="reason", value=reason, inline=False)
    return embed


def parse_policy_dry_run_output(raw_output: str, expected_cities: list[str]) -> tuple[list[DryRunCityBlock], DryRunSummary | None]:
    """Parse script dry-run stdout into city blocks and summary footer."""

    lines = [line.strip() for line in raw_output.splitlines() if line.strip()]
    blocks: list[DryRunCityBlock] = []

    index = 0
    city_idx = 0
    while index + 2 < len(lines):
        first = lines[index]
        if not first.startswith("discord_preview="):
            break
        second = lines[index + 1]
        third = lines[index + 2]
        if not second.startswith("source_candidate_count=") or not third.startswith("primary_candidate="):
            break

        city = expected_cities[city_idx] if city_idx < len(expected_cities) else f"city_{city_idx + 1}"
        preview = first.split("=", 1)[1].strip()
        source_raw = second.split("=", 1)[1].strip()
        primary = third.split("=", 1)[1].strip()
        try:
            source_count = int(source_raw)
        except ValueError:
            source_count = 0

        blocks.append(
            DryRunCityBlock(
                city=city,
                discord_preview=preview,
                source_candidate_count=source_count,
                primary_candidate=primary,
            )
        )
        index += 3
        city_idx += 1

    summary_map: dict[str, str] = {}
    for line in lines[index:]:
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        summary_map[key.strip()] = value.strip()

    summary: DryRunSummary | None = None
    required = {"total_cities", "cities_with_signal", "top_signal_city", "top_signal_candidate"}
    if required.issubset(summary_map.keys()):
        try:
            summary = DryRunSummary(
                total_cities=int(summary_map["total_cities"]),
                cities_with_signal=int(summary_map["cities_with_signal"]),
                top_signal_city=summary_map["top_signal_city"],
                top_signal_candidate=summary_map["top_signal_candidate"],
            )
        except ValueError:
            summary = None

    return blocks, summary


async def run_policy_dry_run(cities: list[str]) -> tuple[list[DryRunCityBlock], DryRunSummary | None]:
    """Run multi-city policy dry-run script and parse its output."""

    command = [
        sys.executable,
        str(POLICY_SCRIPT_PATH),
        "--cities",
        ",".join(cities),
        "--horizon",
        "today",
        "--gamma-limit",
        "3000",
        "--min-abs-edge",
        "0.01",
        "--min-exec-edge",
        "0.0",
        "--discord-dry-run",
    ]

    process = await asyncio.create_subprocess_exec(
        *command,
        cwd=str(POLICY_SCRIPT_PATH.parents[1]),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout_data, stderr_data = await process.communicate()
    if process.returncode != 0:
        error_preview = stderr_data.decode("utf-8", errors="ignore").strip() or "policy dry-run failed"
        raise RuntimeError(error_preview)

    raw_output = stdout_data.decode("utf-8", errors="ignore")
    return parse_policy_dry_run_output(raw_output, cities)


if discord is not None:

    class CitySelect(discord.ui.Select):
        """Dropdown selector for active city in the operator panel."""

        def __init__(self, bot: "DiscordBot") -> None:
            self.bot = bot
            options = [
                discord.SelectOption(label=city.upper(), value=str(index))
                for index, city in enumerate(bot.state.cities)
            ]
            super().__init__(placeholder="Select City", min_values=1, max_values=1, options=options, custom_id="panel_city_select")

        async def callback(self, interaction: discord.Interaction) -> None:
            try:
                selected_index = int(self.values[0])
            except (ValueError, IndexError):
                selected_index = self.bot.state.active_city_index
            self.bot.state.active_city_index = max(0, min(selected_index, len(self.bot.state.cities) - 1))
            _log_panel_event("city_select", f"city={self.bot.state.active_city}")
            await self.bot.panel_view.update_panel(interaction)

    class PanelView(discord.ui.View):
        """Button view for the one-city-at-a-time control panel."""

        def __init__(self, bot: "DiscordBot") -> None:
            super().__init__(timeout=None)
            self.bot = bot
            self.add_item(CitySelect(bot))

        async def update_panel(self, interaction: discord.Interaction, message: str | None = None) -> None:
            """Refresh the current panel message."""

            embed = build_panel_embed(self.bot.state, self.bot.config, status_text=message)
            await interaction.response.edit_message(content=None, embed=embed, view=self)

        @discord.ui.button(label="Refresh Panel", style=discord.ButtonStyle.primary, custom_id="panel_refresh")
        async def refresh(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:  # noqa: ARG002
            _log_panel_event("button_click", "refresh")
            await self.update_panel(interaction)

        @discord.ui.button(label="City ◀", style=discord.ButtonStyle.secondary, custom_id="panel_prev_city")
        async def prev_city(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:  # noqa: ARG002
            self.bot.state.active_city_index = (self.bot.state.active_city_index - 1) % len(self.bot.state.cities)
            _log_panel_event("button_click", f"prev_city -> {self.bot.state.active_city}")
            await self.update_panel(interaction)

        @discord.ui.button(label="City ▶", style=discord.ButtonStyle.secondary, custom_id="panel_next_city")
        async def next_city(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:  # noqa: ARG002
            self.bot.state.active_city_index = (self.bot.state.active_city_index + 1) % len(self.bot.state.cities)
            _log_panel_event("button_click", f"next_city -> {self.bot.state.active_city}")
            await self.update_panel(interaction)

        @discord.ui.button(label="Set Today", style=discord.ButtonStyle.secondary, custom_id="panel_today")
        async def today(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:  # noqa: ARG002
            self.bot.state.active_horizon_index = 0
            _log_panel_event("button_click", "today")
            await self.update_panel(interaction)

        @discord.ui.button(label="Set Tomorrow", style=discord.ButtonStyle.secondary, custom_id="panel_tomorrow")
        async def tomorrow(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:  # noqa: ARG002
            self.bot.state.active_horizon_index = 1
            _log_panel_event("button_click", "tomorrow")
            await self.update_panel(interaction)

        @discord.ui.button(label="Check Positions", style=discord.ButtonStyle.success, custom_id="panel_positions")
        async def positions(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:  # noqa: ARG002
            _log_panel_event("button_click", "positions")
            await interaction.response.send_message(build_status_text("positions", self.bot.state), ephemeral=True)

        @discord.ui.button(label="Check Health", style=discord.ButtonStyle.success, custom_id="panel_health")
        async def health(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:  # noqa: ARG002
            _log_panel_event("button_click", "health")
            await interaction.response.send_message(build_status_text("health", self.bot.state), ephemeral=True)

        @discord.ui.button(label="Toggle Mode", style=discord.ButtonStyle.secondary, custom_id="panel_mode")
        async def mode(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:  # noqa: ARG002
            self.bot.state.mode = "auto+manual" if self.bot.state.mode == "manual-only" else "manual-only"
            _log_panel_event("button_click", f"mode -> {self.bot.state.mode}")
            await self.update_panel(interaction)

        @discord.ui.button(label="Toggle Kill Switch", style=discord.ButtonStyle.danger, custom_id="panel_kill_switch")
        async def kill_switch(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:  # noqa: ARG002
            self.bot.state.kill_switch = not self.bot.state.kill_switch
            _log_panel_event("button_click", f"kill_switch -> {self.bot.state.kill_switch}")
            await self.update_panel(interaction)


    class DiscordBot(commands.Bot):
        """Discord.py bot shell for the operator panel."""

        def __init__(self, token: str | None = None, quiet_ready_log: bool = False) -> None:
            intents = discord.Intents.default()
            super().__init__(command_prefix="!", intents=intents)
            self.token = token
            self.quiet_ready_log = quiet_ready_log
            self.commands_synced = False
            self.panel_message: discord.Message | None = None  # Track persistent panel message
            self.last_daily_run_date: datetime.date | None = None
            self.config = load_config()
            cities = list(self.config.get("cities", {}).keys())
            horizons = list(self.config.get("horizons", ["today", "tomorrow"]))
            self.state = PanelState(
                cities=cities or ["nyc"],
                horizons=horizons or ["today", "tomorrow"],
                mode=self.config.get("mode", "manual-only"),
                kill_switch=bool(self.config.get("kill_switch", False)),
            )
            self.panel_view = PanelView(self)

        async def setup_hook(self) -> None:
            """Register static panel commands."""

            # Register persistent view handlers before gateway events so panel buttons
            # continue to work across process restarts.
            self.add_view(self.panel_view)
            _log_panel_event("persistent_views_registered", "panel_view")
            self.tree.add_command(self._panel_command())
            self.tree.add_command(self._policy_signals_command())

        def _panel_command(self) -> app_commands.Command:
            """Build the slash command that renders the panel."""

            async def panel(interaction: discord.Interaction) -> None:
                embed = build_panel_embed(self.state, self.config)
                await interaction.response.send_message(content=None, embed=embed, view=self.panel_view)

            return app_commands.Command(name="panel", description="Show the weather bot control panel", callback=panel)

        def _policy_signals_command(self) -> app_commands.Command:
            """Build slash command that posts dry-run policy signals for all core cities."""

            async def policy_signals(interaction: discord.Interaction) -> None:
                await interaction.response.defer(thinking=True)

                city_list = list(DEFAULT_SIGNAL_CITIES)
                try:
                    blocks, summary = await run_policy_dry_run(city_list)
                except RuntimeError as exc:
                    error_preview = str(exc).strip() or "policy dry-run failed"
                    await interaction.followup.send(f"Policy dry-run failed: {error_preview}")
                    return

                posted = 0
                for block in blocks:
                    if block.primary_candidate == "NONE":
                        continue
                    await interaction.followup.send(embed=build_signal_embed(block))
                    state, score = _extract_signal_state_score(block.discord_preview)
                    _append_signal_audit_row(
                        city=block.city.upper(),
                        state=state,
                        candidate=block.primary_candidate,
                        score=score,
                        posted=True,
                    )
                    posted += 1

                if posted == 0:
                    await interaction.followup.send("No actionable signal right now for configured cities.")
                    _append_signal_audit_row(
                        city="ALL",
                        state="NO_SIGNAL",
                        candidate="NONE",
                        score="n/a",
                        posted=False,
                    )
                    _log_panel_event("policy_signals", "no_signal")

                if summary is None:
                    summary_lines = [
                        "Daily Policy Summary",
                        f"total_cities={len(blocks)}",
                        f"cities_with_signal={posted}",
                        "top_signal_city=NONE",
                        "top_signal_candidate=NONE",
                    ]
                else:
                    summary_lines = [
                        "Daily Policy Summary",
                        f"total_cities={summary.total_cities}",
                        f"cities_with_signal={summary.cities_with_signal}",
                        f"top_signal_city={summary.top_signal_city}",
                        f"top_signal_candidate={summary.top_signal_candidate}",
                    ]
                await interaction.followup.send("\n".join(summary_lines))
                _log_panel_event("policy_signals", f"posted={posted}")

            return app_commands.Command(
                name="policy_signals",
                description="Post dry-run policy signals for nyc/atlanta/chicago/dallas",
                callback=policy_signals,
            )

        async def on_ready(self) -> None:
            """Report readiness and post persistent panel."""

            if not self.quiet_ready_log:
                print(f"Discord bot ready: {self.user}")
            if self.commands_synced:
                if not self.daily_signal_task.is_running():
                    self.daily_signal_task.start()
                    next_iteration = self.daily_signal_task.next_iteration
                    now_utc = _utc_now()
                    _log_panel_event(
                        "daily_task_boot",
                        (
                            f"startup_utc={now_utc.isoformat()} "
                            f"next_trigger_utc={(next_iteration.isoformat() if next_iteration else 'none')}"
                        ),
                    )
                    _log_panel_event("daily_task", "started")
                return

            guild_env = os.getenv("DISCORD_GUILD_ID")
            try:
                if guild_env:
                    guild_obj = discord.Object(id=int(guild_env))
                    await self.tree.sync(guild=guild_obj)
                else:
                    await self.tree.sync()
                self.commands_synced = True
                if not self.quiet_ready_log:
                    print("Discord commands synced.")
            except Exception as exc:
                if not self.quiet_ready_log:
                    print(f"Discord command sync failed: {exc}")

            # Post persistent panel if configured
            try:
                await self.post_persistent_panel()
            except Exception as exc:
                if not self.quiet_ready_log:
                    print(f"Failed to post persistent panel: {exc}")

            try:
                await self._self_check_signal_channel_permissions()
            except Exception as exc:
                _log_panel_event("permission_check", f"failed={exc}")

            if not self.daily_signal_task.is_running():
                self.daily_signal_task.start()
                next_iteration = self.daily_signal_task.next_iteration
                now_utc = _utc_now()
                _log_panel_event(
                    "daily_task_boot",
                    (
                        f"startup_utc={now_utc.isoformat()} "
                        f"next_trigger_utc={(next_iteration.isoformat() if next_iteration else 'none')}"
                    ),
                )
                _log_panel_event("daily_task", "started")

        async def _run_daily_signal_once(self, trigger_source: str) -> None:
            """Run one daily signal posting cycle with explicit branch diagnostics."""

            now_utc = _utc_now()
            if self.last_daily_run_date == now_utc.date():
                _log_panel_event(
                    "daily_task",
                    f"wakeup_utc={now_utc.isoformat()} source={trigger_source} skipped_already_ran_today",
                )
                return

            channel_id = _resolve_scheduler_channel_id(self.config)
            if channel_id is None:
                _log_panel_event(
                    "daily_task",
                    f"wakeup_utc={now_utc.isoformat()} source={trigger_source} skipped_missing_channel",
                )
                return

            channel = self.get_channel(channel_id)
            if channel is None:
                try:
                    channel = await self.fetch_channel(channel_id)
                except Exception as exc:
                    _log_panel_event(
                        "daily_task",
                        (
                            f"wakeup_utc={now_utc.isoformat()} source={trigger_source} "
                            f"channel_resolve_failed={exc}"
                        ),
                    )
                    return

            if channel is None or not isinstance(channel, discord.abc.Messageable):
                _log_panel_event(
                    "daily_task",
                    f"wakeup_utc={now_utc.isoformat()} source={trigger_source} channel_not_messageable",
                )
                return

            try:
                posted, no_signal = await self._post_policy_signals_to_channel(channel)
                self.last_daily_run_date = now_utc.date()
                if no_signal:
                    _log_panel_event(
                        "daily_task",
                        f"wakeup_utc={now_utc.isoformat()} source={trigger_source} no_signal",
                    )
                else:
                    _log_panel_event(
                        "daily_task",
                        f"wakeup_utc={now_utc.isoformat()} source={trigger_source} posted={posted}",
                    )
            except Exception as exc:
                _log_panel_event(
                    "daily_task",
                    f"wakeup_utc={now_utc.isoformat()} source={trigger_source} failed={exc}",
                )

        async def _self_check_signal_channel_permissions(self) -> None:
            """Log minimal send/history permissions for the configured signal channel."""

            channel_id = _resolve_scheduler_channel_id(self.config)
            if channel_id is None:
                _log_panel_event("permission_check", "skipped_missing_channel")
                return

            channel = self.get_channel(channel_id)
            if channel is None:
                try:
                    channel = await self.fetch_channel(channel_id)
                except Exception as exc:
                    _log_panel_event("permission_check", f"channel_resolve_failed={exc}")
                    return

            if channel is None or not isinstance(channel, discord.abc.GuildChannel):
                _log_panel_event(
                    "permission_check",
                    f"channel={channel_id} view_channel=True send_messages=True read_message_history=True",
                )
                return

            me = channel.guild.me
            if me is None and self.user is not None:
                me = channel.guild.get_member(self.user.id)
            if me is None and self.user is not None:
                try:
                    me = await channel.guild.fetch_member(self.user.id)
                except Exception as exc:
                    _log_panel_event("permission_check", f"member_resolve_failed={exc}")
                    return

            if me is None:
                _log_panel_event("permission_check", "member_unavailable")
                return

            perms = channel.permissions_for(me)
            permission_status = {
                "view_channel": bool(getattr(perms, "view_channel", False)),
                "send_messages": bool(getattr(perms, "send_messages", False)),
                "read_message_history": bool(getattr(perms, "read_message_history", False)),
            }
            _log_panel_event(
                "permission_check",
                (
                    f"channel={channel_id} "
                    f"view_channel={permission_status['view_channel']} "
                    f"send_messages={permission_status['send_messages']} "
                    f"read_message_history={permission_status['read_message_history']}"
                ),
            )

            missing = [name for name, allowed in permission_status.items() if not allowed]
            if missing:
                _log_panel_event("permission_check_missing", ",".join(missing))

        async def _post_policy_signals_to_channel(self, channel: discord.abc.Messageable) -> tuple[int, bool]:
            """Post policy dry-run blocks to a channel and return (posted_count, no_signal)."""

            blocks, summary = await run_policy_dry_run(list(DEFAULT_SIGNAL_CITIES))

            posted = 0
            for block in blocks:
                if block.primary_candidate == "NONE":
                    continue
                await channel.send(embed=build_signal_embed(block))
                state, score = _extract_signal_state_score(block.discord_preview)
                _append_signal_audit_row(
                    city=block.city.upper(),
                    state=state,
                    candidate=block.primary_candidate,
                    score=score,
                    posted=True,
                )
                posted += 1

            no_signal = posted == 0
            if no_signal:
                await channel.send("No actionable signal right now for configured cities.")
                _append_signal_audit_row(
                    city="ALL",
                    state="NO_SIGNAL",
                    candidate="NONE",
                    score="n/a",
                    posted=False,
                )

            if summary is None:
                summary_lines = [
                    "Daily Policy Summary",
                    f"total_cities={len(blocks)}",
                    f"cities_with_signal={posted}",
                    "top_signal_city=NONE",
                    "top_signal_candidate=NONE",
                ]
            else:
                summary_lines = [
                    "Daily Policy Summary",
                    f"total_cities={summary.total_cities}",
                    f"cities_with_signal={summary.cities_with_signal}",
                    f"top_signal_city={summary.top_signal_city}",
                    f"top_signal_candidate={summary.top_signal_candidate}",
                ]
            await channel.send("\n".join(summary_lines))
            return posted, no_signal

        async def post_persistent_panel(self) -> None:
            """Post or update the persistent control panel in the configured channel."""

            panel_channel_id_env = os.getenv("DISCORD_PANEL_CHANNEL_ID")
            if not panel_channel_id_env:
                return  # Not configured, skip

            try:
                channel_id = int(panel_channel_id_env)
            except ValueError:
                return

            channel = self.get_channel(channel_id)
            if channel is None:
                # Try to fetch if not in cache
                try:
                    channel = await self.fetch_channel(channel_id)
                except Exception:
                    return

            if not isinstance(channel, discord.abc.Messageable):
                return

            # Reuse an existing bot panel message when possible to keep a single
            # restart-safe control surface pinned in channel.
            panel_embed = build_panel_embed(self.state, self.config)
            try:
                existing: discord.Message | None = None
                if isinstance(channel, discord.TextChannel):
                    async for msg in channel.history(limit=25):
                        has_panel_title = bool(msg.embeds) and (msg.embeds[0].title == "Weather Bot Control Panel")
                        has_legacy_text = msg.content.startswith("Weather Bot Panel") or msg.content.startswith("Weather Bot Control Panel")
                        if msg.author.id == self.user.id and (has_panel_title or has_legacy_text):
                            existing = msg
                            break

                if existing is not None:
                    self.panel_message = await existing.edit(content=None, embed=panel_embed, view=self.panel_view)
                    _log_panel_event("panel_post", "updated_existing")
                else:
                    self.panel_message = await channel.send(content=None, embed=panel_embed, view=self.panel_view)
                    _log_panel_event("panel_post", "created_new")

                # Try to pin it
                try:
                    await self.panel_message.pin()
                except Exception:
                    pass  # Not critical if pinning fails
                if not self.quiet_ready_log:
                    print(f"Persistent panel posted in channel {channel_id}")
            except Exception as exc:
                if not self.quiet_ready_log:
                    print(f"Failed to post panel: {exc}")

        @tasks.loop(time=dtime(hour=DAILY_TRIGGER_HOUR_UTC, minute=DAILY_TRIGGER_MINUTE_UTC, tzinfo=timezone.utc))
        async def daily_signal_task(self) -> None:
            """Post one daily policy signal batch using the connected bot client."""
            now_utc = _utc_now()
            next_iteration = self.daily_signal_task.next_iteration
            _log_panel_event(
                "daily_task_wakeup",
                (
                    f"current_utc={now_utc.isoformat()} "
                    f"next_trigger_utc={(next_iteration.isoformat() if next_iteration else 'none')}"
                ),
            )
            await self._run_daily_signal_once(trigger_source="scheduled_loop")

        @daily_signal_task.before_loop
        async def before_daily_signal_task(self) -> None:
            """Wait for full client readiness before starting daily task loop."""
            boot_utc = _utc_now()
            _log_panel_event("daily_task_before_loop", f"wait_until_ready_start_utc={boot_utc.isoformat()}")
            await self.wait_until_ready()
            ready_utc = _utc_now()
            next_trigger_utc = _next_daily_trigger_utc(ready_utc)
            _log_panel_event(
                "daily_task_before_loop",
                (
                    f"wait_until_ready_done_utc={ready_utc.isoformat()} "
                    f"next_trigger_utc={next_trigger_utc.isoformat()}"
                ),
            )

            # Reliability catch-up: if readiness happens after today's scheduled minute,
            # execute once immediately so the daily run is not missed.
            today_trigger_utc = ready_utc.replace(
                hour=DAILY_TRIGGER_HOUR_UTC,
                minute=DAILY_TRIGGER_MINUTE_UTC,
                second=0,
                microsecond=0,
            )
            if ready_utc >= today_trigger_utc and self.last_daily_run_date != ready_utc.date():
                _log_panel_event(
                    "daily_task_before_loop",
                    (
                        f"startup_after_target_utc={ready_utc.isoformat()} "
                        f"trigger_utc={today_trigger_utc.isoformat()} catchup_run=true"
                    ),
                )
                await self._run_daily_signal_once(trigger_source="startup_catchup")

        @daily_signal_task.error
        async def daily_signal_task_error(self, exc: Exception) -> None:
            """Log loop-level scheduler errors for diagnostics."""

            _log_panel_event("daily_task_error", f"exception={exc}")

        async def start_bot(self) -> None:
            """Start the bot if a token is configured."""

            if not self.token:
                raise RuntimeError("Discord token is not configured.")
            await self.start(self.token)

        def panel_summary(self) -> str:
            """Return the current text panel state."""

            return build_panel_text(self.state, self.config)

        def publish(self, message: str) -> None:
            """Placeholder transport hook for future integration."""

            raise NotImplementedError(message)
else:

    class DiscordBot:  # type: ignore[no-redef]
        """Fallback stub when discord.py is unavailable."""

        def __init__(self, token: str | None = None, quiet_ready_log: bool = False) -> None:
            self.token = token
            self.quiet_ready_log = quiet_ready_log


def _resolve_scheduler_token(config: dict[str, Any]) -> str | None:
    """Resolve Discord token from config/env for scheduler mode."""

    discord_cfg = config.get("discord", {}) if isinstance(config.get("discord"), dict) else {}
    token_env_name = str(discord_cfg.get("token_env", "DISCORD_BOT_TOKEN"))
    token = os.getenv(token_env_name)
    if token:
        return token.strip()
    direct = discord_cfg.get("token")
    if direct is None:
        return None
    direct_text = str(direct).strip()
    return direct_text or None


def _resolve_scheduler_channel_id(config: dict[str, Any], channel_override: int | None = None) -> int | None:
    """Resolve target Discord channel id from CLI/config/env."""

    if channel_override is not None:
        return channel_override

    discord_cfg = config.get("discord", {}) if isinstance(config.get("discord"), dict) else {}
    env_name = str(discord_cfg.get("signal_channel_id_env", "DISCORD_SIGNAL_CHANNEL_ID"))
    env_value = os.getenv(env_name)
    if env_value:
        try:
            return int(env_value.strip())
        except ValueError:
            return None

    direct_value = discord_cfg.get("signal_channel_id")
    if direct_value is None:
        return None
    try:
        return int(str(direct_value).strip())
    except ValueError:
        return None


async def _post_daily_signals_once(channel_id_override: int | None = None) -> tuple[str, str, str]:
    """Post one daily multi-city signal batch and return compact run summary."""

    if DISCORD_IMPORT_ERROR is not None:
        return "FAILED", "discord_import", DISCORD_IMPORT_ERROR

    failure_stage = "config_load"
    config = load_config()
    failure_stage = "token_config_loaded"
    token = _resolve_scheduler_token(config)
    channel_id = _resolve_scheduler_channel_id(config, channel_override=channel_id_override)
    city_list = list(DEFAULT_SIGNAL_CITIES)

    if not token or not channel_id:
        return "FAILED", "token_config", "missing token or signal channel id"

    try:
        failure_stage = "policy_dry_run"
        blocks, summary = await run_policy_dry_run(city_list)
    except RuntimeError as exc:
        return "FAILED", failure_stage, str(exc)

    # Use a simple Client for scheduler mode
    # Use default intents only - some intents require library-wide declaration
    intents = discord.Intents.default()
    # Only request message content if needed
    intents.message_content = False
    client = discord.Client(intents=intents)

    connect_task = None
    try:
        failure_stage = "discord_login"
        
        # Use login() separately and then connect()
        await client.login(token)
        
        try:
            # Start connect in background
            connect_task = asyncio.create_task(client.connect(reconnect=False))
            
            # Wait for client to be ready (user populated)
            for i in range(300):  # up to 30 seconds
                if client.user is not None:
                    break
                await asyncio.sleep(0.1)
            
            if client.user is None:
                if connect_task:
                    connect_task.cancel()
                raise RuntimeError("client failed to become ready (timeout 30s)")
            
        except asyncio.TimeoutError:
            raise RuntimeError("timeout during client.connect()")
        
        if client.user is None:
            raise RuntimeError("client.user is None after connect")
            
    except asyncio.CancelledError:
        return "FAILED", failure_stage, "login cancelled"
    except discord.errors.LoginFailure as exc:
        return "FAILED", failure_stage, f"login failed: {str(exc)}"
    except Exception as exc:
        try:
            await client.close()
        except Exception:
            pass
        return "FAILED", failure_stage, str(exc)

    try:
        failure_stage = "channel_resolve"
        # Fetch channel using the logged-in client
        try:
            channel = await client.fetch_channel(channel_id)
        except Exception as fetch_exc:
            return "FAILED", failure_stage, f"failed to fetch channel: {str(fetch_exc)}"
        
        if channel is None or not isinstance(channel, discord.abc.Messageable):
            return "FAILED", failure_stage, "target channel not found or not messageable"

        for block in blocks:
            if block.primary_candidate == "NONE":
                continue
            failure_stage = "send_message"
            await channel.send(embed=build_signal_embed(block))
            state, score = _extract_signal_state_score(block.discord_preview)
            _append_signal_audit_row(
                city=block.city.upper(),
                state=state,
                candidate=block.primary_candidate,
                score=score,
                posted=True,
            )
            _log_panel_event("one_shot_post", f"city={block.city} candidate={block.primary_candidate}")

        if not any(block.primary_candidate != "NONE" for block in blocks):
            failure_stage = "send_message"
            await channel.send("No actionable signal right now for configured cities.")
            _append_signal_audit_row(
                city="ALL",
                state="NO_SIGNAL",
                candidate="NONE",
                score="n/a",
                posted=False,
            )
            _log_panel_event("one_shot_post", "no_signal")

        if summary is None:
            summary_text = (
                f"total_cities={len(blocks)}; cities_with_signal=0; "
                "top_signal_city=NONE; top_signal_candidate=NONE"
            )
        else:
            summary_text = (
                f"total_cities={summary.total_cities}; cities_with_signal={summary.cities_with_signal}; "
                f"top_signal_city={summary.top_signal_city}; top_signal_candidate={summary.top_signal_candidate}"
            )
        failure_stage = "send_message"
        await channel.send(
            "\n".join(
                [
                    "Daily Policy Summary",
                    summary_text.replace("; ", "\n"),
                ]
            )
        )
        _log_panel_event("one_shot_post", "summary_sent")
        return "SUCCESS", "NONE", "NONE"
    except Exception as exc:
        return "FAILED", failure_stage, str(exc)
    finally:
        try:
            await client.close()
        except Exception:
            pass
        if connect_task is not None and not connect_task.done():
            connect_task.cancel()


def _parse_cli_args() -> argparse.Namespace:
    """Parse CLI options for scheduler-ready one-shot run."""

    parser = argparse.ArgumentParser(description="Discord bot entrypoint")
    parser.add_argument(
        "--post-daily-signals",
        action="store_true",
        help="Run one-shot daily multi-city signal posting for scheduler use.",
    )
    parser.add_argument(
        "--channel-id",
        type=int,
        default=None,
        help="Optional channel id override for one-shot posting.",
    )
    parser.add_argument(
        "--run-console",
        action="store_true",
        help="Run persistent Discord operator console (slash commands + panel).",
    )
    return parser.parse_args()


async def _run_operator_console() -> int:
    """Run the persistent Discord operator console bot."""

    if DISCORD_IMPORT_ERROR is not None:
        print(f"run_status=FAILED")
        print(f"failure_stage=discord_import")
        print(f"failure_reason={DISCORD_IMPORT_ERROR}")
        return 1

    config = load_config()
    token = _resolve_scheduler_token(config)
    if not token:
        print("run_status=FAILED")
        print("failure_stage=token_config")
        print("failure_reason=missing token")
        return 1

    bot = DiscordBot(token=token, quiet_ready_log=False)
    await bot.start_bot()
    return 0


def main() -> int:
    """CLI entrypoint for scheduler-ready one-shot signal posting."""

    args = _parse_cli_args()
    if args.run_console:
        return asyncio.run(_run_operator_console())

    if not args.post_daily_signals:
        return 0

    run_status, failure_stage, failure_reason = asyncio.run(
        _post_daily_signals_once(channel_id_override=args.channel_id)
    )

    print(f"run_status={run_status}")
    print(f"failure_stage={failure_stage}")
    print(f"failure_reason={failure_reason}")
    return 0 if run_status == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
