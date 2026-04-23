import pytest
from project.core.exceptions import ContractViolationError
from project.core import timeframes


class TestNormalizeTimeframe:
    def test_normalize_minute_timeframes(self):
        assert timeframes.normalize_timeframe("1m") == "1m"
        assert timeframes.normalize_timeframe("5m") == "5m"
        assert timeframes.normalize_timeframe("15m") == "15m"

    def test_normalize_hour_timeframes(self):
        assert timeframes.normalize_timeframe("1h") == "1h"
        assert timeframes.normalize_timeframe("4h") == "4h"

    def test_normalize_day_timeframe(self):
        assert timeframes.normalize_timeframe("1d") == "1d"

    def test_normalize_with_whitespace(self):
        assert timeframes.normalize_timeframe("  5m  ") == "5m"

    def test_normalize_case_insensitive(self):
        assert timeframes.normalize_timeframe("5M") == "5m"
        assert timeframes.normalize_timeframe("1H") == "1h"

    def test_invalid_timeframe_raises_error(self):
        with pytest.raises(ContractViolationError):
            timeframes.normalize_timeframe("2m")
        with pytest.raises(ContractViolationError):
            timeframes.normalize_timeframe("30m")
        with pytest.raises(ContractViolationError):
            timeframes.normalize_timeframe("2h")
        with pytest.raises(ContractViolationError):
            timeframes.normalize_timeframe("1w")
        with pytest.raises(ContractViolationError):
            timeframes.normalize_timeframe("")

    def test_empty_timeframe_raises_error(self):
        with pytest.raises(ContractViolationError):
            timeframes.normalize_timeframe("")


class TestTimeframeToMinutes:
    def test_minute_timeframes(self):
        assert timeframes.timeframe_to_minutes("1m") == 1
        assert timeframes.timeframe_to_minutes("5m") == 5
        assert timeframes.timeframe_to_minutes("15m") == 15

    def test_hour_timeframes(self):
        assert timeframes.timeframe_to_minutes("1h") == 60
        assert timeframes.timeframe_to_minutes("4h") == 240

    def test_day_timeframe(self):
        assert timeframes.timeframe_to_minutes("1d") == 1440

    def test_invalid_timeframe_raises_error(self):
        with pytest.raises(ContractViolationError):
            timeframes.timeframe_to_minutes("2m")


class TestTimeframeToPandasFreq:
    def test_minute_frequencies(self):
        assert timeframes.timeframe_to_pandas_freq("1m") == "1min"
        assert timeframes.timeframe_to_pandas_freq("5m") == "5min"
        assert timeframes.timeframe_to_pandas_freq("15m") == "15min"

    def test_hour_frequencies(self):
        assert timeframes.timeframe_to_pandas_freq("1h") == "1h"
        assert timeframes.timeframe_to_pandas_freq("4h") == "4h"

    def test_day_frequency(self):
        assert timeframes.timeframe_to_pandas_freq("1d") == "1D"

    def test_invalid_timeframe_raises_error(self):
        with pytest.raises(ContractViolationError):
            timeframes.timeframe_to_pandas_freq("2m")


class TestBarsDatasetName:
    def test_minute_bars(self):
        assert timeframes.bars_dataset_name("1m") == "bars_1m"
        assert timeframes.bars_dataset_name("5m") == "bars_5m"
        assert timeframes.bars_dataset_name("15m") == "bars_15m"

    def test_hour_bars(self):
        assert timeframes.bars_dataset_name("1h") == "bars_1h"
        assert timeframes.bars_dataset_name("4h") == "bars_4h"

    def test_day_bars(self):
        assert timeframes.bars_dataset_name("1d") == "bars_1d"

    def test_invalid_timeframe_raises_error(self):
        with pytest.raises(ContractViolationError):
            timeframes.bars_dataset_name("2m")


class TestOhlcvDatasetName:
    def test_minute_ohlcv(self):
        assert timeframes.ohlcv_dataset_name("1m") == "ohlcv_1m"
        assert timeframes.ohlcv_dataset_name("5m") == "ohlcv_5m"
        assert timeframes.ohlcv_dataset_name("15m") == "ohlcv_15m"

    def test_hour_ohlcv(self):
        assert timeframes.ohlcv_dataset_name("1h") == "ohlcv_1h"
        assert timeframes.ohlcv_dataset_name("4h") == "ohlcv_4h"

    def test_day_ohlcv(self):
        assert timeframes.ohlcv_dataset_name("1d") == "ohlcv_1d"

    def test_invalid_timeframe_raises_error(self):
        with pytest.raises(ContractViolationError):
            timeframes.ohlcv_dataset_name("2m")


class TestFundingDatasetName:
    def test_minute_funding(self):
        assert timeframes.funding_dataset_name("1m") == "funding_1m"
        assert timeframes.funding_dataset_name("5m") == "funding_5m"
        assert timeframes.funding_dataset_name("15m") == "funding_15m"

    def test_hour_funding(self):
        assert timeframes.funding_dataset_name("1h") == "funding_1h"
        assert timeframes.funding_dataset_name("4h") == "funding_4h"

    def test_day_funding(self):
        assert timeframes.funding_dataset_name("1d") == "funding_1d"

    def test_invalid_timeframe_raises_error(self):
        with pytest.raises(ContractViolationError):
            timeframes.funding_dataset_name("2m")


class TestBarsPerYear:
    def test_minute_bars_per_year(self):
        assert timeframes.bars_per_year("1m") == 525600
        assert timeframes.bars_per_year("5m") == 105120
        assert timeframes.bars_per_year("15m") == 35040

    def test_hour_bars_per_year(self):
        assert timeframes.bars_per_year("1h") == 8760
        assert timeframes.bars_per_year("4h") == 2190

    def test_day_bars_per_year(self):
        assert timeframes.bars_per_year("1d") == 365

    def test_invalid_timeframe_raises_error(self):
        with pytest.raises(ContractViolationError):
            timeframes.bars_per_year("2m")


class TestIntegration:
    def test_full_pipeline(self):
        tf = "5m"
        normalized = timeframes.normalize_timeframe(tf)
        minutes = timeframes.timeframe_to_minutes(tf)
        freq = timeframes.timeframe_to_pandas_freq(tf)
        bars = timeframes.bars_dataset_name(tf)
        ohlcv = timeframes.ohlcv_dataset_name(tf)
        funding = timeframes.funding_dataset_name(tf)
        bpy = timeframes.bars_per_year(tf)

        assert normalized == "5m"
        assert minutes == 5
        assert freq == "5min"
        assert bars == "bars_5m"
        assert ohlcv == "ohlcv_5m"
        assert funding == "funding_5m"
        assert bpy == 105120
