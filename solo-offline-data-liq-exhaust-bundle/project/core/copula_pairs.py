from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_SPEC_PATH = _REPO_ROOT / 'spec' / 'copula_pairs.csv'


def _canonical_symbol(symbol: object) -> str:
    return str(symbol or '').strip().upper()


def _canonical_pair(a: object, b: object) -> tuple[str, str]:
    left = _canonical_symbol(a)
    right = _canonical_symbol(b)
    if not left or not right:
        raise ValueError('copula pair symbols must be non-empty')
    if left == right:
        raise ValueError(f'copula pair must contain two distinct symbols: {left}')
    return tuple(sorted((left, right)))


@lru_cache(maxsize=8)
def load_copula_pairs(path: str | Path | None = None) -> pd.DataFrame:
    source = Path(path) if path is not None else _DEFAULT_SPEC_PATH
    if not source.exists():
        raise FileNotFoundError(f'copula pairs spec not found: {source}')
    frame = pd.read_csv(source)
    if frame.empty:
        return pd.DataFrame(columns=['symbol_a', 'symbol_b', 'pair_id'])
    missing = {'symbol_a', 'symbol_b'} - set(frame.columns)
    if missing:
        raise ValueError(f'copula pairs spec {source} missing required columns: {sorted(missing)}')
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in frame[['symbol_a', 'symbol_b']].to_dict(orient='records'):
        pair = _canonical_pair(row['symbol_a'], row['symbol_b'])
        if pair in seen:
            continue
        seen.add(pair)
        rows.append({'symbol_a': pair[0], 'symbol_b': pair[1], 'pair_id': f'{pair[0]}__{pair[1]}'})
    return pd.DataFrame(rows, columns=['symbol_a', 'symbol_b', 'pair_id'])


def copula_pair_universe(path: str | Path | None = None) -> set[str]:
    pairs = load_copula_pairs(path=path)
    if pairs.empty:
        return set()
    return set(pairs['symbol_a'].astype(str)).union(set(pairs['symbol_b'].astype(str)))


def copula_partners(symbol: str, path: str | Path | None = None) -> list[str]:
    token = _canonical_symbol(symbol)
    if not token:
        return []
    pairs = load_copula_pairs(path=path)
    if pairs.empty:
        return []
    partners = set()
    for row in pairs.to_dict(orient='records'):
        a = row['symbol_a']
        b = row['symbol_b']
        if token == a:
            partners.add(b)
        elif token == b:
            partners.add(a)
    return sorted(partners)
