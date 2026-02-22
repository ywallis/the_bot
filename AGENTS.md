# AGENTS.md - Guidelines for AI Agents

This document provides guidelines for AI agents working in this repository.

## Project Overview

This is a Python-based event-driven liquidity arbitrage framework. The project uses:
- Python 3.12+
- pytest with pytest-asyncio for testing
- ruff for linting (docstrings only)
- mypy for type checking
- CCXT for cryptocurrency exchange integrations

## Build, Lint, and Test Commands

### Running Tests

Run all tests:
```bash
pytest
```

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

Run ruff linter:
```bash
ruff check apps/
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

- Use Python 3.12+ union syntax (`str | None` instead of `Optional[str]`)
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
│   └── src/           # Shared utilities
└── accountant/
    ├── src/
    └── tests/
```

### Misc

- Use `match/case` (structural pattern matching) for type-safe conditionals
- Use `Decimal` for all financial calculations (never float)
- Keep functions focused and single-purpose
- Add type annotations to all function signatures and variables
