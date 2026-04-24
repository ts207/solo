from __future__ import annotations

from project.live.ingest.parsers import parse_book_ticker_event, parse_kline_event


def test_parse_kline_event_valid():
    payload = {
        "e": "kline",
        "E": 123456789,
        "s": "BTCUSDT",
        "k": {
            "t": 123400000,
            "T": 123459999,
            "s": "BTCUSDT",
            "i": "1m",
            "f": 100,
            "L": 200,
            "o": "0.0010",
            "c": "0.0020",
            "h": "0.0025",
            "l": "0.0015",
            "v": "1000",
            "n": 100,
            "x": False,
            "q": "1.0000",
            "V": "500",
            "Q": "0.500",
            "B": "123456",
        },
    }

    event = parse_kline_event(payload)
    assert event is not None
    assert event.symbol == "BTCUSDT"
    assert event.timeframe == "1m"
    assert event.open == 0.0010
    assert event.close == 0.0020
    assert event.high == 0.0025
    assert event.low == 0.0015
    assert event.volume == 1000.0
    assert event.quote_volume == 1.0
    assert event.taker_base_volume == 500.0
    assert event.is_final is False


def test_parse_book_ticker_event_combined():
    payload = {
        "stream": "btcusdt@bookTicker",
        "data": {
            "e": "bookTicker",
            "u": 400900217,
            "s": "BTCUSDT",
            "b": "25.3519",
            "B": "31.21",
            "a": "25.3652",
            "A": "40.66",
            "T": 1672531200000,
            "E": 1672531200005,
        },
    }
    event = parse_book_ticker_event(payload)
    assert event is not None
    assert event.symbol == "BTCUSDT"
    assert event.best_bid_price == 25.3519
    assert event.best_bid_qty == 31.21
    assert event.best_ask_price == 25.3652
    assert event.best_ask_qty == 40.66


def test_parse_book_ticker_event_single():
    payload = {
        "u": 400900217,
        "s": "BTCUSDT",
        "b": "25.3519",
        "B": "31.21",
        "a": "25.3652",
        "A": "40.66",
    }
    event = parse_book_ticker_event(payload)
    assert event is not None
    assert event.symbol == "BTCUSDT"
    assert event.best_bid_price == 25.3519
    assert event.best_bid_qty == 31.21
    assert event.best_ask_price == 25.3652
    assert event.best_ask_qty == 40.66
