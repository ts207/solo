import os
import pandas as pd
import pytest
from pathlib import Path
from project.research.phase2 import load_features, _FEATURE_CACHE, clear_feature_cache

def test_load_features_caching(tmp_path):
    # Setup mock data directory
    data_root = tmp_path
    run_id = "test_run"
    symbol = "BTCUSDT"
    timeframe = "5m"
    market = "perp"
    
    # Create mock parquet file
    feature_dir = data_root / "lake" / "features" / market / symbol / timeframe / "features_feature_schema_v2"
    feature_dir.mkdir(parents=True)
    
    df_original = pd.DataFrame({
        "timestamp": pd.to_datetime(["2024-01-01 00:00:00", "2024-01-01 00:05:00"], utc=True),
        "close": [100.0, 101.0]
    })
    df_original.to_parquet(feature_dir / "data.parquet")
    
    # Clear cache before starting
    clear_feature_cache()
    
    # 1. First call - should load from disk
    df1 = load_features(data_root, run_id, symbol, timeframe, market=market)
    assert not df1.empty
    assert len(_FEATURE_CACHE) == 1
    
    # 2. Second call - should load from cache
    # We can verify this by deleting the file from disk
    (feature_dir / "data.parquet").unlink()
    
    df2 = load_features(data_root, run_id, symbol, timeframe, market=market)
    assert not df2.empty
    pd.testing.assert_frame_equal(df1, df2)
    
    # 3. Verify it's a copy
    df2.at[0, "close"] = 999.0
    assert df1.at[0, "close"] == 100.0
    
    # 4. Third call - verify the cache itself wasn't mutated
    df3 = load_features(data_root, run_id, symbol, timeframe, market=market)
    assert df3.at[0, "close"] == 100.0

def test_cache_key_tuple(tmp_path):
    # Verify different parameters result in different cache entries
    data_root = tmp_path
    run_id = "test_run"
    symbol = "BTCUSDT"
    timeframe = "5m"
    market = "perp"
    
    clear_feature_cache()
    
    # Helper to create data
    def create_data(sym, tf):
        d = data_root / "lake" / "features" / market / sym / tf / "features_feature_schema_v2"
        d.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({
            "timestamp": pd.to_datetime(["2024-01-01 00:00:00"], utc=True),
            "close": [100.0]
        }).to_parquet(d / "data.parquet")

    create_data("BTCUSDT", "5m")
    create_data("ETHUSDT", "5m")
    create_data("BTCUSDT", "15m")
    
    load_features(data_root, run_id, "BTCUSDT", "5m", market=market)
    load_features(data_root, run_id, "ETHUSDT", "5m", market=market)
    load_features(data_root, run_id, "BTCUSDT", "15m", market=market)
    
    assert len(_FEATURE_CACHE) == 3
    assert ("test_run", "BTCUSDT", "5m", "perp", ()) in _FEATURE_CACHE
    assert ("test_run", "ETHUSDT", "5m", "perp", ()) in _FEATURE_CACHE
    assert ("test_run", "BTCUSDT", "15m", "perp", ()) in _FEATURE_CACHE

def test_cache_key_higher_timeframes(tmp_path):
    # Verify higher_timeframes affects cache key
    data_root = tmp_path
    run_id = "test_run"
    symbol = "BTCUSDT"
    timeframe = "5m"
    market = "perp"
    
    clear_feature_cache()
    
    # Helper to create data
    def create_data(sym, tf):
        d = data_root / "lake" / "features" / market / sym / tf / "features_feature_schema_v2"
        d.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({
            "timestamp": pd.to_datetime(["2024-01-01 00:00:00"], utc=True),
            "close": [100.0]
        }).to_parquet(d / "data.parquet")

    create_data("BTCUSDT", "5m")
    create_data("BTCUSDT", "15m")
    create_data("BTCUSDT", "1h")
    
    # 1. No higher timeframes
    load_features(data_root, run_id, symbol, timeframe, market=market)
    assert len(_FEATURE_CACHE) == 1
    assert (run_id, symbol, timeframe, market, ()) in _FEATURE_CACHE
    
    # 2. With higher timeframes
    load_features(data_root, run_id, symbol, timeframe, market=market, higher_timeframes=["15m"])
    # Note: load_features calls itself recursively for HTFs, so they also get cached.
    # Base call: (run_id, symbol, "5m", "perp", ("15m",))
    # Recursive call: (run_id, symbol, "15m", "perp", ())
    assert (run_id, symbol, timeframe, market, ("15m",)) in _FEATURE_CACHE
    assert (run_id, symbol, "15m", market, ()) in _FEATURE_CACHE
    
    # 3. With different higher timeframes
    load_features(data_root, run_id, symbol, timeframe, market=market, higher_timeframes=["1h"])
    assert (run_id, symbol, timeframe, market, ("1h",)) in _FEATURE_CACHE
    assert (run_id, symbol, "1h", market, ()) in _FEATURE_CACHE
    
    # Check that unsorted lists result in same cache key (tuple is sorted in implementation)
    load_features(data_root, run_id, symbol, timeframe, market=market, higher_timeframes=["1h", "15m"])
    load_features(data_root, run_id, symbol, timeframe, market=market, higher_timeframes=["15m", "1h"])
    assert (run_id, symbol, timeframe, market, ("15m", "1h")) in _FEATURE_CACHE
    # No extra entry should be created for the second call
    
def test_cache_inactive_if_not_testing(tmp_path, monkeypatch):
    # Temporarily unset the env var
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    
    data_root = tmp_path
    run_id = "test_run"
    symbol = "BTCUSDT"
    timeframe = "5m"
    market = "perp"
    
    feature_dir = data_root / "lake" / "features" / market / symbol / timeframe / "features_feature_schema_v2"
    feature_dir.mkdir(parents=True)
    pd.DataFrame({
        "timestamp": pd.to_datetime(["2024-01-01 00:00:00"], utc=True),
        "close": [100.0]
    }).to_parquet(feature_dir / "data.parquet")
    
    _FEATURE_CACHE.clear()
    
    # Should load but NOT cache
    df = load_features(data_root, run_id, symbol, timeframe, market=market)
    assert not df.empty
    assert len(_FEATURE_CACHE) == 0
