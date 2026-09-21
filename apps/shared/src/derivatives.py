"""Derivatives account setup: margin mode, leverage and position mode.

A short that hedges inventory is only the position the design describes if
it is opened at the configured leverage, in the configured margin mode and
in one-way position mode. None of those are properties of an order; they
are properties of the account per contract symbol, set once before the
first order and checked on every start. This module sets them from the
venue's config and refuses to let a venue trade if it cannot. See
``docs/design/inventory-hedging.md`` section 6.
"""

import logging
from typing import Any

from ccxt.async_support import NoChange, NotSupported

from apps.shared.src.config import VenueConfig, is_contract

logger = logging.getLogger(__name__)


class DerivativesSetupError(Exception):
    """Raised when a venue's derivatives settings could not be applied."""


async def _apply(
    client: Any, venue: VenueConfig, symbol: str, what: str, call: Any
) -> str | None:
    """
    Run one setter and classify the outcome.

    Parameters
    ----------
    client : Any
        The CCXT client.
    venue : VenueConfig
        The venue, for messages.
    symbol : str
        The contract symbol being configured.
    what : str
        What is being set, for messages.
    call : Any
        Zero-argument coroutine function performing the call.

    Returns
    -------
    str | None
        A description of what was applied, None if the venue reported the
        setting was already in place.

    Raises
    ------
    DerivativesSetupError
        If the venue refused the setting for any other reason.
    """
    try:
        await call()
    except NoChange:
        # ``MarginModeAlreadySet`` is a ``NoChange``: the account is where
        # the config wants it, which is the point of setting it on start.
        logger.debug(f"{venue.id} {symbol}: {what} already set")
        return None
    except Exception as error:  # noqa: BLE001, re-raised with context
        raise DerivativesSetupError(
            f"{venue.id} {symbol}: could not set {what}: {error}"
        ) from error
    return f"{symbol} {what}"


async def configure_derivatives(
    client: Any, venue: VenueConfig, symbols: set[str]
) -> list[str]:
    """
    Apply a venue's margin mode, leverage and one-way position mode.

    Runs on every contract symbol the venue trades, in that order, each
    guarded by the client's ``has`` map. A spot venue, or a derivatives
    venue with neither leverage nor margin mode configured, is left alone:
    the settings are opt-in, and a venue that does not expose a setter is
    skipped with a warning rather than failed, since an account can be
    configured by hand.

    Parameters
    ----------
    client : Any
        The venue's authenticated CCXT client, markets loaded.
    venue : VenueConfig
        The venue, whose ``margin_mode`` and ``leverage`` are applied.
    symbols : set[str]
        The symbols the venue trades; spot symbols are ignored.

    Returns
    -------
    list[str]
        What was applied, one entry per setter that changed something.

    Raises
    ------
    DerivativesSetupError
        If any setter was refused. The caller must not let the venue trade
        after this: an order at the wrong leverage or margin mode is worse
        than no order.
    """
    contracts = sorted(s for s in symbols if is_contract(s))
    if not venue.derivatives or not contracts:
        return []
    if venue.margin_mode is None and venue.leverage is None:
        logger.info(f"{venue.id}: no margin_mode or leverage configured, not set")
        return []

    has = getattr(client, "has", {}) or {}
    applied: list[str] = []
    for symbol in contracts:
        if venue.margin_mode is not None:
            if has.get("setMarginMode"):
                params: dict[str, Any] = {}
                if venue.leverage is not None:
                    # Some venues require the leverage alongside an isolated
                    # margin mode; the others ignore the extra parameter.
                    params["leverage"] = venue.leverage
                result = await _apply(
                    client,
                    venue,
                    symbol,
                    f"margin mode {venue.margin_mode}",
                    lambda s=symbol, p=params: client.set_margin_mode(
                        venue.margin_mode, s, params=p
                    ),
                )
                if result:
                    applied.append(result)
            else:
                logger.warning(
                    f"{venue.id} cannot set a margin mode through CCXT; "
                    f"make sure {symbol} is {venue.margin_mode} by hand"
                )
        if venue.leverage is not None:
            if has.get("setLeverage"):
                # One call per declared parameter set: a venue that keeps
                # leverage per margin type and position side needs one for
                # each side the hedge may hold. No parameters is one call.
                for extra in venue.leverage_params or ({},):
                    what = f"leverage {venue.leverage}"
                    if extra:
                        what += " " + " ".join(f"{k}={v}" for k, v in extra.items())
                    result = await _apply(
                        client,
                        venue,
                        symbol,
                        what,
                        lambda s=symbol, p=extra: client.set_leverage(
                            venue.leverage, s, params=dict(p)
                        ),
                    )
                    if result:
                        applied.append(result)
            else:
                logger.warning(
                    f"{venue.id} cannot set leverage through CCXT; make sure "
                    f"{symbol} is at {venue.leverage}x by hand"
                )
        if has.get("setPositionMode"):
            try:
                result = await _apply(
                    client,
                    venue,
                    symbol,
                    "one-way position mode",
                    lambda s=symbol: client.set_position_mode(False, s),
                )
            except DerivativesSetupError as error:
                if isinstance(error.__cause__, NotSupported):
                    logger.warning(f"{error}; assuming one-way")
                    continue
                raise
            if result:
                applied.append(result)
    for entry in applied:
        logger.info(f"{venue.id}: set {entry}")
    return applied
