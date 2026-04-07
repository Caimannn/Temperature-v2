"""SQLite storage adapter for market and signal logs."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class BotDatabase:
    """Small SQLite wrapper with explicit schema and insert helpers."""

    path: str = "data/weather_bot.sqlite3"

    def init_db(self) -> None:
        """Create required tables if they do not already exist."""

        db_path = Path(self.path)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS market_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city_key TEXT NOT NULL,
                    horizon TEXT NOT NULL,
                    target_day TEXT NOT NULL,
                    market_slug TEXT,
                    range_label TEXT,
                    side TEXT,
                    price REAL,
                    observed_at TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    raw_json TEXT
                );

                CREATE TABLE IF NOT EXISTS weather_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city_key TEXT NOT NULL,
                    horizon TEXT NOT NULL,
                    target_day TEXT NOT NULL,
                    market_slug TEXT,
                    range_label TEXT,
                    side TEXT,
                    price REAL,
                    observed_at TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    raw_json TEXT
                );

                CREATE TABLE IF NOT EXISTS signal_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city_key TEXT NOT NULL,
                    horizon TEXT NOT NULL,
                    target_day TEXT NOT NULL,
                    market_slug TEXT,
                    range_label TEXT,
                    side TEXT,
                    price REAL,
                    signal_text TEXT,
                    observed_at TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    raw_json TEXT
                );

                CREATE TABLE IF NOT EXISTS position_advice_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city_key TEXT NOT NULL,
                    horizon TEXT NOT NULL,
                    target_day TEXT NOT NULL,
                    market_slug TEXT,
                    range_label TEXT,
                    side TEXT,
                    price REAL,
                    advice_action TEXT,
                    advice_text TEXT,
                    observed_at TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    raw_json TEXT
                );
                """
            )

    def insert_market_snapshot(
        self,
        city_key: str,
        horizon: str,
        target_day: str,
        market_slug: str | None,
        range_label: str | None,
        side: str | None,
        price: float | None,
        observed_at: str | None = None,
        raw: dict[str, Any] | None = None,
    ) -> int:
        """Insert one market snapshot row."""

        return self._insert_common(
            table="market_snapshots",
            city_key=city_key,
            horizon=horizon,
            target_day=target_day,
            market_slug=market_slug,
            range_label=range_label,
            side=side,
            price=price,
            observed_at=observed_at,
            raw=raw,
        )

    def insert_weather_snapshot(
        self,
        city_key: str,
        horizon: str,
        target_day: str,
        market_slug: str | None,
        range_label: str | None,
        side: str | None,
        price: float | None,
        observed_at: str | None = None,
        raw: dict[str, Any] | None = None,
    ) -> int:
        """Insert one weather snapshot row."""

        return self._insert_common(
            table="weather_snapshots",
            city_key=city_key,
            horizon=horizon,
            target_day=target_day,
            market_slug=market_slug,
            range_label=range_label,
            side=side,
            price=price,
            observed_at=observed_at,
            raw=raw,
        )

    def insert_signal_log(
        self,
        city_key: str,
        horizon: str,
        target_day: str,
        market_slug: str | None,
        range_label: str | None,
        side: str | None,
        price: float | None,
        signal_text: str,
        observed_at: str | None = None,
        raw: dict[str, Any] | None = None,
    ) -> int:
        """Insert one signal log row."""

        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO signal_logs (
                    city_key,
                    horizon,
                    target_day,
                    market_slug,
                    range_label,
                    side,
                    price,
                    signal_text,
                    observed_at,
                    raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    city_key,
                    horizon,
                    target_day,
                    market_slug,
                    range_label,
                    side,
                    price,
                    signal_text,
                    observed_at,
                    _to_json(raw),
                ),
            )
            return int(cursor.lastrowid)

    def insert_position_advice_log(
        self,
        city_key: str,
        horizon: str,
        target_day: str,
        market_slug: str | None,
        range_label: str | None,
        side: str | None,
        price: float | None,
        advice_action: str,
        advice_text: str,
        observed_at: str | None = None,
        raw: dict[str, Any] | None = None,
    ) -> int:
        """Insert one position advice log row."""

        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO position_advice_logs (
                    city_key,
                    horizon,
                    target_day,
                    market_slug,
                    range_label,
                    side,
                    price,
                    advice_action,
                    advice_text,
                    observed_at,
                    raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    city_key,
                    horizon,
                    target_day,
                    market_slug,
                    range_label,
                    side,
                    price,
                    advice_action,
                    advice_text,
                    observed_at,
                    _to_json(raw),
                ),
            )
            return int(cursor.lastrowid)

    def _insert_common(
        self,
        table: str,
        city_key: str,
        horizon: str,
        target_day: str,
        market_slug: str | None,
        range_label: str | None,
        side: str | None,
        price: float | None,
        observed_at: str | None,
        raw: dict[str, Any] | None,
    ) -> int:
        """Insert into a table that follows the common snapshot schema."""

        with self._connect() as connection:
            cursor = connection.execute(
                f"""
                INSERT INTO {table} (
                    city_key,
                    horizon,
                    target_day,
                    market_slug,
                    range_label,
                    side,
                    price,
                    observed_at,
                    raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    city_key,
                    horizon,
                    target_day,
                    market_slug,
                    range_label,
                    side,
                    price,
                    observed_at,
                    _to_json(raw),
                ),
            )
            return int(cursor.lastrowid)

    def _connect(self) -> sqlite3.Connection:
        """Open a SQLite connection."""

        return sqlite3.connect(self.path)


def _to_json(value: dict[str, Any] | None) -> str | None:
    """Serialize a dictionary to JSON for raw storage."""

    if value is None:
        return None
    return json.dumps(value, separators=(",", ":"), ensure_ascii=True)
