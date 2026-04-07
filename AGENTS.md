# AGENTS

## 1. Repo Purpose
- Modular Python bot for analyzing Polymarket weather temperature markets.
- Focus on daily temperature bins (today/tomorrow horizons).
- Core task: compare model probabilities vs market-implied probabilities.
- Output explainable signal candidates before any execution work.

## 2. Working Rules
- Prefer the smallest possible change.
- Change only files required for the task.
- Keep layers separated and modular.
- Do not mix model logic, market logic, Discord UI, and execution in one patch.
- Do not perform broad refactors unless explicitly requested.

## 3. Implementation Style
- Prefer pure functions and typed dataclasses.
- Keep parsing explicit and easy to audit.
- Keep diagnostics visible in outputs and return objects.
- Preserve existing public contracts unless explicitly asked to change them.
- Prefer one small new module over large edits across unrelated modules.

## 4. Scope Guardrails
- Do not add trading execution unless explicitly requested.
- Do not add Discord changes unless explicitly requested.
- Do not add CLOB/order-book logic unless explicitly requested.
- Do not introduce ML-heavy or overengineered solutions.
- Default to explainable baseline logic.

## 5. Testing Expectations
- For each new engine layer, add or suggest a small manual runner in scripts/.
- After changes, always report:
  - files touched
  - what changed
  - what was intentionally left unchanged
  - how to test manually

## 6. Repo-Specific Anti-Patterns
- Giant multi-file rewrites.
- Hidden magic values.
- Mixing business logic with presentation.
- Duplicate helper logic.
- Speculative features not requested.
- Adding execution before model validation.

## 7. Preferred Workflow
- First provide a short plan in 3-6 bullets.
- Then implement the smallest patch.
- Then show manual test steps.
- If the request is ambiguous, choose the narrowest interpretation.
