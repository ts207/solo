from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class VenueSymbolRules:
    symbol: str
    tick_size: float | None = None
    step_size: float | None = None
    min_qty: float | None = None
    min_notional: float | None = None
    market_order_supported: bool = True
    limit_order_supported: bool = True
    reduce_only_supported: bool = True
    post_only_supported: bool = False
    source: str = "configured"

    @classmethod
    def from_mapping(cls, symbol: str, payload: Mapping[str, Any]) -> VenueSymbolRules:
        def _optional_positive(*keys: str) -> float | None:
            for key in keys:
                if key not in payload:
                    continue
                try:
                    value = float(payload.get(key))
                except (TypeError, ValueError):
                    continue
                if math.isfinite(value) and value > 0.0:
                    return value
            return None

        return cls(
            symbol=str(symbol).upper(),
            tick_size=_optional_positive("tick_size", "tickSize"),
            step_size=_optional_positive("step_size", "qty_step", "qtyStep", "stepSize"),
            min_qty=_optional_positive("min_qty", "min_order_qty", "minOrderQty", "minQty"),
            min_notional=_optional_positive("min_notional", "min_notional_usd", "minNotional"),
            market_order_supported=bool(payload.get("market_order_supported", True)),
            limit_order_supported=bool(payload.get("limit_order_supported", True)),
            reduce_only_supported=bool(payload.get("reduce_only_supported", True)),
            post_only_supported=bool(payload.get("post_only_supported", False)),
            source=str(payload.get("source", "configured") or "configured"),
        )

    @property
    def is_actionable(self) -> bool:
        return (
            self.tick_size is not None
            and self.step_size is not None
            and self.min_qty is not None
            and self.min_notional is not None
        )


@dataclass(frozen=True)
class VenueRuleCheck:
    accepted: bool
    quantity: float
    price: float | None
    blocked_by: str = ""
    reasons: tuple[str, ...] = ()
    diagnostics: dict[str, Any] | None = None


def _round_down(value: float, step: float | None) -> float:
    if step is None or step <= 0.0:
        return float(value)
    precision = max(0, -int(math.floor(math.log10(step))))
    return round(math.floor(float(value) / step) * step, precision)


def _round_price(price: float, tick_size: float | None, side: str) -> float:
    if tick_size is None or tick_size <= 0.0:
        return float(price)
    precision = max(0, -int(math.floor(math.log10(tick_size))))
    units = float(price) / tick_size
    if str(side).strip().lower() == "sell":
        rounded = math.ceil(units) * tick_size
    else:
        rounded = math.floor(units) * tick_size
    return round(rounded, precision)


def check_and_normalize_order(
    *,
    rules: VenueSymbolRules,
    order_type: str,
    side: str,
    quantity: float,
    reference_price: float,
    limit_price: float | None = None,
    reduce_only: bool = False,
    post_only: bool = False,
) -> VenueRuleCheck:
    reasons: list[str] = []
    normalized_qty = _round_down(max(0.0, float(quantity)), rules.step_size)

    if str(order_type).strip().lower() == "market" and not rules.market_order_supported:
        reasons.append("market_order_not_supported")
    if str(order_type).strip().lower() == "limit" and not rules.limit_order_supported:
        reasons.append("limit_order_not_supported")
    if reduce_only and not rules.reduce_only_supported:
        reasons.append("reduce_only_not_supported")
    if post_only and not rules.post_only_supported:
        reasons.append("post_only_not_supported")

    min_qty = float(rules.min_qty or 0.0)
    if min_qty > 0.0 and normalized_qty < min_qty:
        reasons.append("below_min_qty")

    price_for_notional = float(limit_price or reference_price or 0.0)
    normalized_price = None
    if limit_price is not None:
        normalized_price = _round_price(float(limit_price), rules.tick_size, side)
        price_for_notional = normalized_price

    min_notional = float(rules.min_notional or 0.0)
    notional = normalized_qty * abs(float(price_for_notional))
    if min_notional > 0.0 and notional < min_notional:
        reasons.append("below_min_notional")

    diagnostics = {
        "venue_rule_source": rules.source,
        "tick_size": rules.tick_size,
        "step_size": rules.step_size,
        "min_qty": rules.min_qty,
        "min_notional": rules.min_notional,
        "requested_quantity": float(quantity),
        "normalized_quantity": normalized_qty,
        "reference_price": float(reference_price),
        "notional": notional,
    }
    if reasons:
        return VenueRuleCheck(
            accepted=False,
            quantity=0.0,
            price=normalized_price,
            blocked_by="venue_rules",
            reasons=tuple(reasons),
            diagnostics=diagnostics,
        )
    return VenueRuleCheck(
        accepted=True,
        quantity=normalized_qty,
        price=normalized_price,
        diagnostics=diagnostics,
    )


def load_configured_venue_rules(
    symbols: list[str],
    strategy_runtime: Mapping[str, Any],
) -> dict[str, VenueSymbolRules]:
    raw = strategy_runtime.get("venue_rules") or strategy_runtime.get("symbol_rules") or {}
    if not isinstance(raw, Mapping):
        return {}

    out: dict[str, VenueSymbolRules] = {}
    default_payload = raw.get("default", {}) if isinstance(raw.get("default", {}), Mapping) else {}
    for symbol in [str(item).upper() for item in symbols]:
        symbol_payload = raw.get(symbol) or raw.get(symbol.lower()) or {}
        if not isinstance(symbol_payload, Mapping):
            continue
        merged = dict(default_payload) | dict(symbol_payload)
        if merged:
            out[symbol] = VenueSymbolRules.from_mapping(symbol, merged)
    return out


def parse_binance_venue_rules(
    payload: Mapping[str, Any],
    symbols: list[str],
) -> dict[str, VenueSymbolRules]:
    requested = {str(item).upper() for item in symbols}
    rows = payload.get("symbols", [])
    if not isinstance(rows, list):
        return {}
    out: dict[str, VenueSymbolRules] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        symbol = str(row.get("symbol", "")).upper()
        if not symbol or symbol not in requested:
            continue
        filters = {
            str(item.get("filterType", "")): item
            for item in row.get("filters", [])
            if isinstance(item, Mapping)
        }
        price_filter = filters.get("PRICE_FILTER", {})
        lot_filter = filters.get("LOT_SIZE", {})
        notional_filter = filters.get("MIN_NOTIONAL", {}) or filters.get("NOTIONAL", {})
        order_types = set(row.get("orderTypes") or [])
        rule = VenueSymbolRules.from_mapping(
            symbol,
            {
                "tickSize": price_filter.get("tickSize"),
                "stepSize": lot_filter.get("stepSize"),
                "minQty": lot_filter.get("minQty"),
                "minNotional": notional_filter.get("notional")
                or notional_filter.get("minNotional"),
                "market_order_supported": "MARKET" in order_types,
                "limit_order_supported": "LIMIT" in order_types,
                "reduce_only_supported": True,
                "post_only_supported": True,
                "source": "exchange:binance",
            },
        )
        out[symbol] = rule
    return out


def parse_bybit_venue_rules(
    payload: Mapping[str, Any],
    symbols: list[str],
) -> dict[str, VenueSymbolRules]:
    requested = {str(item).upper() for item in symbols}
    rows = payload.get("list", [])
    if not isinstance(rows, list):
        return {}
    out: dict[str, VenueSymbolRules] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        symbol = str(row.get("symbol", "")).upper()
        if not symbol or symbol not in requested:
            continue
        price_filter = row.get("priceFilter", {})
        lot_filter = row.get("lotSizeFilter", {})
        if not isinstance(price_filter, Mapping):
            price_filter = {}
        if not isinstance(lot_filter, Mapping):
            lot_filter = {}
        status = str(row.get("status", "")).strip().lower()
        trading = status in {"", "trading"}
        rule = VenueSymbolRules.from_mapping(
            symbol,
            {
                "tickSize": price_filter.get("tickSize"),
                "qtyStep": lot_filter.get("qtyStep"),
                "minOrderQty": lot_filter.get("minOrderQty"),
                "minNotional": lot_filter.get("minNotionalValue"),
                "market_order_supported": trading,
                "limit_order_supported": trading,
                "reduce_only_supported": True,
                "post_only_supported": True,
                "source": "exchange:bybit",
            },
        )
        out[symbol] = rule
    return out


async def fetch_exchange_venue_rules(
    *,
    exchange: str,
    rest_client: Any,
    symbols: list[str],
) -> dict[str, VenueSymbolRules]:
    normalized_exchange = str(exchange).strip().lower()
    normalized_symbols = [str(symbol).upper() for symbol in symbols]
    if normalized_exchange == "binance":
        payload = await rest_client.get_exchange_info()
        if isinstance(payload, Mapping):
            return parse_binance_venue_rules(payload, normalized_symbols)
        return {}
    if normalized_exchange == "bybit":
        out: dict[str, VenueSymbolRules] = {}
        for symbol in normalized_symbols:
            payload = await rest_client.get_instruments_info(symbol=symbol)
            if isinstance(payload, Mapping):
                out.update(parse_bybit_venue_rules(payload, [symbol]))
        return out
    return {}


def merge_venue_rule_sources(
    exchange_rules: Mapping[str, VenueSymbolRules],
    configured_rules: Mapping[str, VenueSymbolRules],
) -> dict[str, VenueSymbolRules]:
    merged = dict(configured_rules)
    for symbol, rule in exchange_rules.items():
        if rule.is_actionable:
            merged[str(symbol).upper()] = rule
    return merged
