# Repository Guidelines

## Project Structure & Module Organization
- `bot/` hosts the aiogram bot: routers in `handlers/`, keyboards in `keyboards/`, middleware in `middleware/`, and shared helpers in `utils/`. `bot/main.py` is the orchestration entry point.
- `services/` (e.g., `vk_multibot_service.py`, `excel_service.py`) isolates external APIs; `db_module/` contains asynchronous PostgreSQL access layers and is the place for schema-aware changes.
- Automation scripts stay in the repo root (`setup_project.py`, `cleanup_project.py`) or `scripts/`; keep conversational logic inside `bot/`.
- Generated artifacts (`data/`, `logs/`, `exports/`, `backups/`) are ephemeral and should remain untracked.

## Build, Test, and Development Commands
- Prepare the environment with `python -m venv .venv && source .venv/bin/activate` and `pip install -r requirements.txt`.
- Launch the bot locally via `python run.py`; use `.env` to supply Telegram, VK, and database credentials.
- Run diagnostics with the provided scripts, e.g., `python test_cache_logic.py` or `python test_force_search.py`, to verify cache, search, and balance flows after changes.
- Tidy stateful resources with `python cleanup_project.py` when rotating sessions or clearing test data.

## Coding Style & Naming Conventions
- Follow PEP 8, four-space indentation, and descriptive snake_case identifiers (`vk_cache_service`, `force_search_job`). Keep async/await flow consistent with existing handlers.
- Maintain Russian copy and emoji styling for user-facing responses; place translation strings alongside their handlers for clarity.
- Add type hints and docstrings when amending services or database adapters; reuse existing dataclasses and response dictionaries for interoperability.

## Testing Guidelines
- Tests live as `test_*.py` scripts in the project root; execute them directly (`python test_balance_fix.py`) or through Pytest (`python -m pytest test_cache_logic.py`) for aggregated reporting.
- Preserve the naming pattern `test_<area>_<detail>.py` and close Telethon clients in teardowns to avoid session leakage.
- Store reproducible fixtures in `data/`, scrub personal data, and use environment variables for secrets referenced by tests.

## Commit & Pull Request Guidelines
- Mirror the current history: prefix commits with `DD.MM.YYYY` followed by a brief Russian summary of the change.
- Keep related edits (handlers, services, migrations) together and reference issue IDs or task links when available.
- Pull requests should outline the change, list verification commands/logs, and call out configuration or schema impacts, plus screenshots for user-visible tweaks.

## Security & Configuration Tips
- Seed configuration with `cp .env.example .env`, keep real credentials locally, and avoid committing session files (`user_session*.session`, `auth_*.log`).
- Rotate PostgreSQL/Redis secrets when sharing environments, and ensure directories created by `bot/main.py` (`DATA_DIR`, `DEBUG_DIR`) have restricted permissions before deployment.


- Каждую доработку описывай и создавай файл с названием доработки 