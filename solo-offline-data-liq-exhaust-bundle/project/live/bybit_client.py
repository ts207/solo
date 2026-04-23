from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
from typing import Any, Dict, List, Optional

import aiohttp

_LOG = logging.getLogger(__name__)


class _AsyncTokenBucket:
    def __init__(self, capacity: float, refill_per_second: float):
        self.capacity = float(max(1.0, capacity))
        self.refill_per_second = float(max(0.0, refill_per_second))
        self._tokens = float(self.capacity)
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, cost: float = 1.0) -> None:
        cost = float(max(0.0, cost))
        async with self._lock:
            while True:
                now = time.monotonic()
                elapsed = now - self._updated
                if elapsed > 0.0:
                    self._tokens = min(
                        self.capacity, self._tokens + elapsed * self.refill_per_second
                    )
                    self._updated = now
                if self._tokens >= cost:
                    self._tokens -= cost
                    return
                deficit = cost - self._tokens
                wait_time = deficit / self.refill_per_second if self.refill_per_second > 0 else 0.25
                await asyncio.sleep(max(0.05, wait_time))


class BybitDerivativesClient:
    """
    Bybit V5 Derivatives REST Client using aiohttp.
    """

    BASE_URL = "https://api.bybit.com"

    def __init__(self, api_key: str, api_secret: str, *, base_url: str | None = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.BASE_URL = str(base_url or self.BASE_URL).rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()
        # Bybit rate limits are generally generous, but let's keep a conservative default.
        self._rate_limiter = _AsyncTokenBucket(capacity=10.0, refill_per_second=10.0)

    async def close(self):
        if self._session:
            await self._session.close()
            self._session = None

    async def _get_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session is None or self._session.closed:
                timeout = aiohttp.ClientTimeout(total=30)
                self._session = aiohttp.ClientSession(timeout=timeout)
            return self._session

    def _sign(self, timestamp: str, recv_window: str, payload: str) -> str:
        msg = timestamp + self.api_key + recv_window + payload
        return hmac.new(
            self.api_secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256
        ).hexdigest()

    _MAX_RETRIES = 5

    async def _request(
        self,
        method: str,
        path: str,
        params: Dict[str, Any] | None = None,
        signed: bool = False,
        _retry_count: int = 0,
    ) -> Any:
        url = f"{self.BASE_URL}{path}"
        headers = {
            "Content-Type": "application/json",
        }

        recv_window = "5000"
        payload = ""

        if method == "GET" and params:
            from urllib.parse import urlencode

            query_string = urlencode(params)
            url = f"{url}?{query_string}"
            payload = query_string
        elif method == "POST" and params:
            payload = json.dumps(params)

        if signed:
            timestamp = str(int(time.time() * 1000))
            signature = self._sign(timestamp, recv_window, payload)
            headers.update(
                {
                    "X-BAPI-API-KEY": self.api_key,
                    "X-BAPI-TIMESTAMP": timestamp,
                    "X-BAPI-RECV-WINDOW": recv_window,
                    "X-BAPI-SIGN": signature,
                }
            )

        await self._rate_limiter.acquire()
        session = await self._get_session()

        rate_limited = False
        wait_time = 1.0

        async with session.request(
            method,
            url,
            data=payload if method == "POST" else None,
            headers=headers,
        ) as resp:
            data = await resp.json()
            if resp.status == 429:
                if _retry_count >= self._MAX_RETRIES:
                    _LOG.error(
                        "Bybit rate-limited on %s %s; max retries (%d) exhausted",
                        method,
                        path,
                        self._MAX_RETRIES,
                    )
                    resp.raise_for_status()
                # X-Bapi-Limit-Reset-Timestamp is epoch milliseconds
                reset_ms = float(resp.headers.get("X-Bapi-Limit-Reset-Timestamp", 0))
                if reset_ms > 0:
                    now_ms = time.time() * 1000
                    wait_time = max(0.5, (reset_ms - now_ms) / 1000.0)
                else:
                    wait_time = 1.0
                rate_limited = True
            elif resp.status != 200:
                _LOG.error(f"Bybit API Error: {resp.status} {data}")
                resp.raise_for_status()
            elif data.get("retCode") != 0:
                _LOG.error(f"Bybit API Error: {data.get('retCode')} {data.get('retMsg')}")
                raise Exception(f"Bybit Error: {data.get('retMsg')} ({data.get('retCode')})")
            else:
                return data.get("result")

        # Retry after rate-limit (outside the response context manager)
        if rate_limited:
            _LOG.warning(
                "Bybit rate-limited on %s %s; retrying in %.2fs (attempt %d/%d)",
                method,
                path,
                wait_time,
                _retry_count + 1,
                self._MAX_RETRIES,
            )
            await asyncio.sleep(wait_time)
            return await self._request(
                method, path, params=params, signed=signed, _retry_count=_retry_count + 1
            )

    async def get_wallet_balance(self, account_type: str = "UNIFIED") -> Dict[str, Any]:
        """GET /v5/account/wallet-balance"""
        params = {"accountType": account_type}
        return await self._request("GET", "/v5/account/wallet-balance", params=params, signed=True)

    async def get_klines(
        self,
        symbol: str,
        interval: str,
        limit: int = 200,
        category: str = "linear",
    ) -> List[List[Any]]:
        """GET /v5/market/kline"""
        # Map interval from Binance-style (1m, 5m) to Bybit-style if needed.
        # Bybit V5 kline intervals: 1, 3, 5, 15, 30, 60, 120, 240, 360, 720, D, M, W
        bybit_interval = interval
        if interval.endswith("m"):
            bybit_interval = interval[:-1]
        elif interval == "1h":
            bybit_interval = "60"
        elif interval == "4h":
            bybit_interval = "240"
        elif interval == "1d":
            bybit_interval = "D"

        params = {
            "category": category,
            "symbol": symbol.upper(),
            "interval": bybit_interval,
            "limit": limit,
        }
        res = await self._request("GET", "/v5/market/kline", params=params)
        return res.get("list", [])

    async def get_tickers(
        self, symbol: str | None = None, category: str = "linear"
    ) -> List[Dict[str, Any]]:
        """GET /v5/market/tickers"""
        params = {"category": category}
        if symbol:
            params["symbol"] = symbol.upper()
        res = await self._request("GET", "/v5/market/tickers", params=params)
        return res.get("list", [])

    async def get_instruments_info(
        self, symbol: str | None = None, category: str = "linear"
    ) -> Dict[str, Any]:
        """GET /v5/market/instruments-info"""
        params = {"category": category}
        if symbol:
            params["symbol"] = symbol.upper()
        return await self._request("GET", "/v5/market/instruments-info", params=params)

    async def get_premium_index(self, symbol: str | None = None) -> Dict[str, Any]:
        """Return mark price and funding rate in the same schema the runner expects.

        Bybit V5 exposes these on the tickers endpoint rather than a dedicated
        premiumIndex endpoint.  We re-map field names to match the Binance
        premiumIndex response so the runner's _fetch_runtime_market_features_from_rest
        can work unchanged against both exchanges.
        """
        tickers = await self.get_tickers(symbol=symbol)
        if not tickers:
            return {}
        t = tickers[0]
        return {
            "markPrice": t.get("markPrice", "0"),
            "lastFundingRate": t.get("fundingRate", "0"),
            "nextFundingTime": t.get("nextFundingTime", "0"),
            "time": t.get("nextFundingTime", "0"),
        }

    async def get_open_interest(
        self,
        symbol: str,
        interval: str = "5min",
        limit: int = 50,
        category: str = "linear",
    ) -> Dict[str, Any]:
        """GET /v5/market/open-interest

        Returns the most-recent data point in Binance-compatible flat format
        {"openInterest": <str>, "time": <int>} so the runner can read it the
        same way regardless of exchange.
        """
        params = {
            "category": category,
            "symbol": symbol.upper(),
            "intervalTime": interval,
            "limit": limit,
        }
        result = await self._request("GET", "/v5/market/open-interest", params=params)
        rows = result.get("list", []) if isinstance(result, dict) else []
        if not rows:
            return {}
        latest = rows[0]
        return {
            "openInterest": latest.get("openInterest", "0"),
            "time": int(latest.get("timestamp", 0)),
        }

    async def get_funding_rate_history(
        self,
        symbol: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 200,
        category: str = "linear",
    ) -> List[Dict[str, Any]]:
        """GET /v5/market/funding/history"""
        params = {
            "category": category,
            "symbol": symbol.upper(),
            "limit": limit,
        }
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        res = await self._request("GET", "/v5/market/funding/history", params=params)
        return res.get("list", [])

    async def get_mark_price_klines(
        self, symbol: str, interval: str, limit: int = 200
    ) -> List[List[Any]]:
        """GET /v5/market/mark-price-kline"""
        return await self._request_kline_variant(
            "/v5/market/mark-price-kline", symbol, interval, limit
        )

    async def _request_kline_variant(
        self,
        path: str,
        symbol: str,
        interval: str,
        limit: int = 200,
        category: str = "linear",
    ) -> List[List[Any]]:
        bybit_interval = interval
        if interval.endswith("m"):
            bybit_interval = interval[:-1]
        elif interval == "1h":
            bybit_interval = "60"
        elif interval == "4h":
            bybit_interval = "240"
        elif interval == "1d":
            bybit_interval = "D"

        params = {
            "category": category,
            "symbol": symbol.upper(),
            "interval": bybit_interval,
            "limit": limit,
        }
        res = await self._request("GET", path, params=params)
        return res.get("list", [])

    async def get_mark_price_klines_v2(
        self, symbol: str, interval: str, limit: int = 200
    ) -> List[List[Any]]:
        return await self._request_kline_variant(
            "/v5/market/mark-price-kline", symbol, interval, limit
        )

    async def get_index_price_klines(
        self, symbol: str, interval: str, limit: int = 200
    ) -> List[List[Any]]:
        return await self._request_kline_variant(
            "/v5/market/index-price-kline", symbol, interval, limit
        )

    async def cancel_all_open_orders(self, symbol: str, category: str = "linear") -> Any:
        """POST /v5/order/cancel-all"""
        params = {
            "category": category,
            "symbol": symbol.upper(),
        }
        return await self._request("POST", "/v5/order/cancel-all", params=params, signed=True)

    async def create_market_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        reduce_only: bool = False,
        new_client_order_id: str | None = None,
        category: str = "linear",
    ) -> Any:
        """POST /v5/order/create"""
        params = {
            "category": category,
            "symbol": symbol.upper(),
            "side": side.capitalize(),
            "orderType": "Market",
            "qty": str(quantity),
            "reduceOnly": reduce_only,
        }
        if new_client_order_id:
            params["orderLinkId"] = str(new_client_order_id)

        return await self._request("POST", "/v5/order/create", params=params, signed=True)

    async def create_limit_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        time_in_force: str = "GTC",
        reduce_only: bool = False,
        post_only: bool = False,
        new_client_order_id: str | None = None,
        category: str = "linear",
    ) -> Any:
        """POST /v5/order/create"""
        tif = "PostOnly" if post_only else time_in_force
        params = {
            "category": category,
            "symbol": symbol.upper(),
            "side": side.capitalize(),
            "orderType": "Limit",
            "qty": str(quantity),
            "price": str(price),
            "timeInForce": tif,
            "reduceOnly": reduce_only,
        }
        if new_client_order_id:
            params["orderLinkId"] = str(new_client_order_id)

        return await self._request("POST", "/v5/order/create", params=params, signed=True)
