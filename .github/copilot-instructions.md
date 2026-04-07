# Copilot Instructions

## 1. Project Purpose
- Build a modular Python bot that analyzes Polymarket weather markets for daily temperature outcomes.
- Focus on highest-temperature markets for today and tomorrow.
- Estimate model probabilities from weather providers and compare them to market-implied probabilities.
- Produce explainable signal candidates first; execution comes later.

## 2. Architecture Rules
- Keep one responsibility per module.
- Keep layers separated:
	weather providers -> weather aggregation -> weather probability -> market comparison -> signal candidates.
- Do not mix weather collection, probability modeling, market comparison, Discord UI, and execution in one patch.
- Prefer adding one small file over editing many unrelated files.
- Do not refactor broadly unless explicitly requested.

## 3. Editing Rules For Copilot
- Before coding, provide a short plan in 3-6 bullets.
- Touch only files needed for the requested task.
- Keep patches minimal, local, and reversible.
- Preserve existing public contracts unless explicitly asked to change them.
- Do not rename files/functions without a concrete reason.
- Reuse existing helpers; do not duplicate logic.

## 4. Code Size Discipline
- Avoid large files and wide-scope edits.
- If a file grows too much, extract small helpers.
- Prefer compact dataclasses and pure functions.
- Prefer deterministic, explainable logic over clever abstractions.
- Do not introduce heavy frameworks.

## 5. Python Style
- Use typed dataclasses where they improve clarity.
- Prefer pure functions in engine layers.
- Keep side effects in scripts and integration modules.
- Handle missing/invalid data explicitly.
- Keep parsing and diagnostics visible and testable.

## 6. Testing Discipline
- For each new engine layer, add a small manual script in scripts/ when practical.
- Print debug-friendly output for manual verification.
- Do not add trading logic or extra network scope unless the task asks for it.

## 7. Scope Control
- Default to the smallest implementation that satisfies the request.
- Never add Discord, CLOB, execution, or trading changes unless explicitly requested.
- If a prompt is ambiguous, choose the narrowest interpretation and state what is intentionally not changed.

## 8. Output Expectations
- After each coding task, report:
	files touched,
	what changed,
	what was intentionally left unchanged,
	and how to test manually.

## 9. Anti-Patterns To Avoid
- Giant multi-file rewrites.
- Mixing business logic with Discord output.
- Hidden magic values.
- Overengineering.
- Speculative features not requested.
- Adding market execution before model/compare layers are validated.
