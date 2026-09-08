# AGENTS.md - Guidelines for AI Agents

This document provides guidelines for AI agents working in this repository.

## Project Overview

This is a Python-based event-driven liquidity arbitrage framework. The project uses:
- Python 3.14+
- pytest with pytest-asyncio for testing
- ruff for linting (docstrings only)
- ty for type checking
- CCXT for cryptocurrency exchange integrations

## Confidentiality

This repository is public. Never name the venues traded on or the assets
traded, in code, tests, fixtures, documentation, commit messages or pull
request descriptions. That information can be used against the operator.
Refer to venues and symbols generically (`venue_a`, `BASE/QUOTE`, "the maker
venue") and read the real ones from `config.toml` at runtime; venue-specific
behaviour belongs behind a config option or the CCXT `has` map, never a
hardcoded venue id. The strategies repository (`apps/strategies`) is private
and is where venue and asset names live.

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

Coverage is not on by default, so a plain `pytest` run stays fast and a
filtered run (`pytest -k ...`) does not trip the floor. CI runs:
```bash
uv run pytest -q --cov --cov-report=term
```

`[tool.coverage.run]` measures the four `src` trees with branch coverage;
`fail_under` in `[tool.coverage.report]` is a ratchet set to what the suite
reaches today, not a target. `apps/accountant/src` has no tests at all and is
what holds the number down; the other three trees sit around 79%. Raise the
floor as coverage improves, and do not lower it to make a red build green.

### Linting

Run ruff linter (installed as a dev dependency):
```bash
uv run ruff check apps/
```

Formatting is `ruff format`, and CI gates on it:
```bash
uv run ruff format apps/          # apply
uv run ruff format --check apps/  # what CI runs
```

`apps/strategies` is excluded from ruff: it is a separate private repository
and its style is its own business. ty and pytest do still cover it, so a
submodule bump that breaks types or tests fails here.

The project only lints docstrings (D rules), ignoring D100 (missing module docstring):
```toml
[tool.ruff.lint]
select = ["D"]
ignore = ["D100"]

[tool.ruff.lint.pydocstyle]
convention = "numpy"
```

### Type Checking

Run ty (installed as a dev dependency):
```bash
uv run ty check --exit-zero-on-warning apps/
```

`apps/*/src` is expected to stay clean. `--exit-zero-on-warning` keeps
warning-level diagnostics visible without failing the run; drop it to see
whether a warning is one you introduced.

Test files are checked more loosely: `[[tool.ty.overrides]]` in
`pyproject.toml` turns off the rules that only fire because redis-py reply
types are broad unions and because test doubles stand in for real clients.
Tightening that means typed reply helpers in `conftest.py`, not per-line
suppressions.

Suppress a diagnostic with `# ty: ignore[rule-name]`, using ty's rule name
(`invalid-argument-type`, not mypy's `arg-type`). ty reports an ignore that
suppresses nothing, so stale ones do not accumulate.

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
│   │   ├── runtime.py      # Strategy runtime: Clock, Strategy base class, Runtime (XREAD loop, submit/cancel)
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
- `apps/maker/src/order_watcher.py` turns CCXT `watch_orders` updates into `OrderEvent`s and, after every websocket drop, reconciles over REST (`fetch_orders` or open plus cancelled-and-closed, per the venue's `has` map) so an order that finished during the gap is still reported; `apps/maker/src/matcher.py` is an ordinary consumer of `oms:events` and holds no exchange connection.
- `apps/maker/src/orchestrator.py` stops the system in phases (`SHUTDOWN_PHASES`): strategies, order manager, feeds and broker, recorder. A new long-running module must be added to a phase or it stops with the feeds; the order manager must never be stopped after the broker or before the recorder.
- Strategies are written against `apps/shared/src/runtime.py`: subclass `Strategy`, override the `on_*` hooks, build intents with `runtime.order_intent(...)` and `submit` them. A strategy module exports a `STRATEGY` class; the launcher constructs it with its `StrategyConfig`. Strategy and slot identifiers must not contain `_` or `-`, which the client order id format splits on.
- A quote's `replace_of` is the intent it supersedes, or `REPLACE_RESTING` (empty string) for the first quote of a slot; `None` means an independent order that shutdown leaves alone. The order manager clears whatever rests under the strategy key on any superseding intent, not only the order named.
- There is no legacy strategy path any more: the `messageprocessor` pubsub channel, `legacy_bridge.py` and the manual strategy simulators are gone. Anything that wants an order placed publishes an intent to `oms:intents`. The `OrderMessage`/`CancellationMessage` TypedDicts remain only because the broker still speaks its pubsub protocol to the order manager.
- Strategy tests drive the real `Runtime` on `fakeredis` through the `Harness` in `apps/strategies/tests/conftest.py` and assert on `oms:intents`; do not mock Redis calls inside a strategy.
- Consumers of a stream resolve its tail with `streams.stream_tail` and then advance through concrete entry ids. Do not pass `$` to a repeated `XREAD`: it re-resolves per call and silently drops anything published between two reads.
- Redis replies are **bytes** in every live process: `decode_responses` passed to `Redis(...)` is ignored when an existing `ConnectionPool` is handed in, and the pools here are built without it. Pass any stream entry id back through `streams.entry_id_str` before using it as a command argument — `str(b"1-0")` is `"b'1-0'"`, which Redis rejects. Tests that build a client directly get `str` instead, so cover both (see the parametrised fixtures in `test_message_processor.py`).
- Before adding a venue, after a CCXT upgrade, or when a feed misbehaves: run `uv run -m apps.maker.src.tools.venue_conformance <venue> <SYMBOL>` and follow `docs/runbooks/new-venue.md`. Venue websocket behaviour (checksums, trade ids, cache replay, error types) is not covered by unit tests.
- Design and migration plan: `docs/design/event-driven-framework.md`.

### Misc

- Use `match/case` (structural pattern matching) for type-safe conditionals
- Use `Decimal` for all financial calculations (never float)
- Keep functions focused and single-purpose
- Add type annotations to all function signatures and variables
