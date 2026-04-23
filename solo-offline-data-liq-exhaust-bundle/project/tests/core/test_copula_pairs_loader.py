from __future__ import annotations

from project.core.copula_pairs import copula_pair_universe, copula_partners, load_copula_pairs


def test_load_copula_pairs_deduplicates_and_normalizes() -> None:
    pairs = load_copula_pairs()
    assert list(pairs.columns) == ["symbol_a", "symbol_b", "pair_id"]
    assert len(pairs) >= 3
    assert set(pairs["symbol_a"]) | set(pairs["symbol_b"]) >= {"BTCUSDT", "ETHUSDT", "SOLUSDT"}
    assert all("__" in pair_id for pair_id in pairs["pair_id"])


def test_copula_partners_are_sorted_and_symbol_specific() -> None:
    partners = copula_partners("BTCUSDT")
    assert partners == sorted(partners)
    assert set(partners) == {"ETHUSDT", "SOLUSDT", "BNBUSDT"}


def test_copula_pair_universe_contains_all_symbols() -> None:
    universe = copula_pair_universe()
    assert {"BTCUSDT", "ETHUSDT", "SOLUSDT"} <= universe
