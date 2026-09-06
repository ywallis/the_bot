# AGENTS.md - Guidelines for AI Agents

This document provides guidelines for AI agents working in this repository.

## Project Overview

This is a Python-based event-driven liquidity arbitrage framework. The project uses:
- Python 3.14+
- pytest with pytest-asyncio for testing
- ruff for linting (docstrings only)
- mypy for type checking
- CCXT for cryptocurrency exchange integrations

## Build, Lint, and Test Commands

### Running Tests

The strategies submodule holds `config.toml`, which most modules load at
import time. In a fresh clone or worktree run `git submodule update --init`
first or the maker tests fail at collection.

Run all tests:
```bash
pytest
```

Stream and Redis behaviour is tested against `fakeredis` (a dev dependency),
so no Redis server is needed.

Run a single test file:
```bash
pytest apps/maker/tests/test_utils.py
```

Run a single test function:
```bash
pytest apps/maker/tests/test_utils.py::test_parse_message
```

Run tests matching a pattern:
```bash
pytest -k "test_parse"
```

Run tests with verbose output:
```bash
pytest -v
```

Run tests with coverage (if configured):
```bash
pytest --cov=apps/maker/src --cov=apps/shared/src
```

### Linting

Run ruff linter (installed as a dev dependency):
```bash
uv run ruff check apps/
```

The project only lints docstrings (D rules), ignoring D100 (missing module docstring):
```toml
[tool.ruff.lint]
select = ["D"]
ignore = ["D100"]

[tool.ruff.lint.pydocstyle]
convention = "numpy"
```

### Type Checking

Run mypy:
```bash
mypy apps/
```

The project uses these mypy settings (from pyproject.toml):
- Python path: `apps/maker/src`
- Extensions: mypy-extensions

## Code Style Guidelines

### Imports

- Use absolute imports with the `apps.*` prefix:
  ```python
  from apps.maker.src.enums import MessageType, OrderSide
  from apps.shared.src.errors import ExchangeError
  ```
- Group imports in this order: standard library, third-party, local/application
- Within each group, sort alphabetically

### Formatting

- Maximum line length: 88 characters (ruff default)
- Use 4 spaces for indentation (no tabs)
- Use numpy-style docstrings (see below)
- Add blank lines between top-level definitions

### Docstrings (NumPy Convention)

All public functions and classes must have numpydoc-style docstrings:

```python
def function_name(param1: str, param2: int) -> bool:
    """
    Short description of what the function does.

    Parameters
    ----------
    param1 : str
        Description of param1.
    param2 : int
        Description of param2.

    Returns
    -------
    bool
        Description of the return value.

    Raises
    ------
    CustomError
        When something specific fails.
    """
```

### Type Annotations

- Use Python 3.10+ union syntax (`str | None` instead of `Optional[str]`)
- Use `TypeGuard` for type narrowing functions
- Use `TypedDict` for dictionary-based data structures
- Use `Decimal` for financial quantities (not float)
- Use `cast()` from typing when type checker needs persuasion

### Naming Conventions

- **Classes**: `PascalCase` (e.g., `OrderMessage`, `CustomEnum`)
- **Functions/variables**: `snake_case` (e.g., `parse_message`, `order_raw`)
- **Constants**: `UPPER_SNAKE_CASE` (e.g., `MAX_SIZE`)
- **Enums**: Values should be lowercase strings (e.g., `ORDER = "order"`)

### Error Handling

- Use specific exception types from `ccxt.async_support` (imported via `apps.shared.src.errors`)
- Re-export commonly used exceptions:
  ```python
  from apps.shared.src.errors import (
      BadRequest,
      ExchangeError,
      InvalidOrder,
      NetworkError,
      RequestTimeout,
  )
  ```
- Raise descriptive exceptions with context:
  ```python
  raise Exception("Invalid OID!")
  ```

### Enums

Use the custom `CustomEnum` base class which provides `__str__` returning the value:

```python
class MessageType(CustomEnum):
    """Description of the enum."""

    ORDER = "order"
    CANCELLATION = "cancellation"
```

### Data Structures (TypedDict)

Use `TypedDict` for structured dictionaries:

```python
class OrderMessage(TypedDict):
    """Description of the dict."""

    kind: MessageType
    strategy: str
    price: Decimal
```

Access TypedDict fields using bracket notation: `order["strategy"]`

### Async Code

- Use `pytest_asyncio` for async fixtures
- Configure asyncio test mode in conftest.py or pyproject.toml:
  ```python
  @pytest_asyncio.fixture
  async def my_fixture():
      ...
  ```
- Use `asyncio.DefaultEventLoopPolicy` for async operations

### Testing

- Place tests in `apps/*/tests/` directories
- Use pytest fixtures defined in `conftest.py`
- Use `pytest-mock` for mocking
- Test file naming: `test_*.py`
- Test function naming: `test_*`
- Use `deepcopy` in fixtures to prevent mutation between tests

### File Organization

```
apps/
├── maker/
│   ├── src/           # Source code
│   └── tests/         # Test files
├── shared/
│   ├── src/           # Shared utilities
│   │   ├── config.py       # Typed config loader (msgspec). Single source of truth for config.toml
│   │   ├── events.py       # Event schemas, JSON codec and Redis Stream names (cross-language contract)
│   │   ├── ccxt_events.py  # Converters from CCXT structures to Book/Trade/Balance/OrderEvent
│   │   ├── streams.py      # StreamPublisher (XADD with seq and trimming), stream_tail, enumeration, paths
│   │   └── utils.py        # Backwards-compatible module globals derived from config.py
│   └── tests/
└── accountant/
    ├── src/
    └── tests/
```

### Configuration and events

- Read config through `apps.shared.src.config.load_app_config()`; do not parse `config.toml` elsewhere.
- Every strategy declares `subscriptions`; parameters live under `[strategies.params]`. Stray top-level keys are a config error.
- New inter-process messages are `msgspec.Struct` events in `apps.shared.src.events` and travel on Redis Streams.
- Feed handlers (`apps/maker/src/watcher.py`, `balance.py`) publish through `StreamPublisher` and keep writing the legacy snapshot keys.
- `apps/maker/src/recorder.py` tails every configured stream into `data/<stream path>/<UTC hour>.jsonl`; it is the hot tier of the recording, shipped to the archive host once sealed. Never touch the newest file in a stream directory: the recorder still holds it open.
- Orders travel as events: strategies publish `OrderIntent`/`CancelIntent` to `oms:intents`, the order manager (`apps/maker/src/message_processor.py`) is the single consumer of that stream through the `oms` consumer group, and it publishes `OrderEvent`s to `oms:events` and `LatencyRecord`s to `oms:latency`.
- `apps/maker/src/order_watcher.py` turns CCXT `watch_orders` updates into `OrderEvent`s; `apps/maker/src/matcher.py` is an ordinary consumer of `oms:events` and holds no exchange connection.
- Legacy strategies still publish dicts on the `messageprocessor` pubsub channel. `apps/maker/src/legacy_bridge.py` translates them into intents and is the only place that knows the legacy wire format; delete it in phase 4 rather than adding a second path through the order manager.
- Consumers of a stream resolve its tail with `streams.stream_tail` and then advance through concrete entry ids. Do not pass `$` to a repeated `XREAD`: it re-resolves per call and silently drops anything published between two reads.
- Redis replies are **bytes** in every live process: `decode_responses` passed to `Redis(...)` is ignored when an existing `ConnectionPool` is handed in, and the pools here are built without it. Pass any stream entry id back through `streams.entry_id_str` before using it as a command argument — `str(b"1-0")` is `"b'1-0'"`, which Redis rejects. Tests that build a client directly get `str` instead, so cover both (see the parametrised fixtures in `test_message_processor.py`).
- Before adding a venue, after a CCXT upgrade, or when a feed misbehaves: run `uv run -m apps.maker.src.tools.venue_conformance <venue> <SYMBOL>` and follow `docs/runbooks/new-venue.md`. Venue websocket behaviour (checksums, trade ids, cache replay, error types) is not covered by unit tests.
- Design and migration plan: `docs/design/event-driven-framework.md`.

### Misc

- Use `match/case` (structural pattern matching) for type-safe conditionals
- Use `Decimal` for all financial calculations (never float)
- Keep functions focused and single-purpose
- Add type annotations to all function signatures and variables
