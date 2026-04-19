from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

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


class BinanceFuturesClient:
    """
    Binance USD-M Futures REST Client using aiohttp.
    """

    BASE_URL = "https://fapi.binance.com"

    def __init__(self, api_key: str, api_secret: str, *, base_url: str | None = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.BASE_URL = str(base_url or self.BASE_URL).rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()
        # Binance weights vary by endpoint; this conservative default keeps unwind bursts bounded.
        self._rate_limiter = _AsyncTokenBucket(capacity=20.0, refill_per_second=15.0)

    async def close(self):
        if self._session:
            await self._session.close()
            self._session = None

    def _sign(self, params: Dict[str, Any]) -> str:
        query_string = urlencode(params)
        return hmac.new(
            self.api_secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256
        ).hexdigest()

    async def _get_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session is None or self._session.closed:
                timeout = aiohttp.ClientTimeout(total=30)
                self._session = aiohttp.ClientSession(timeout=timeout)
            return self._session

    async def _request(
        self, method: str, path: str, params: Dict[str, Any] = None, signed: bool = False
    ) -> Any:
        raw_params = dict(params or {})
        params = dict(raw_params)
        if signed:
            params["timestamp"] = int(time.time() * 1000)
            query_string = urlencode(params)
            params["signature"] = hmac.new(
                self.api_secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256
            ).hexdigest()

        headers = {"X-MBX-APIKEY": self.api_key}
        url = f"{self.BASE_URL}{path}"

        await self._rate_limiter.acquire()
        session = await self._get_session()
        async with session.request(method, url, params=params, headers=headers) as resp:
            data = await resp.json()
            if resp.status == 429:
                retry_after = float(resp.headers.get("Retry-After", 5))
                _LOG.warning(
                    "Binance rate-limited on %s %s; retrying in %.2fs", method, path, retry_after
                )
                await asyncio.sleep(max(0.5, retry_after))
                return await self._request(method, path, params=raw_params, signed=signed)
            if resp.status != 200:
                _LOG.error(f"Binance API Error: {resp.status} {data}")
                resp.raise_for_status()
            return data

    async def get_account_v2(self) -> Dict[str, Any]:
        """GET /fapi/v2/account"""
        return await self._request("GET", "/fapi/v2/account", signed=True)

    async def get_klines(self, symbol: str, interval: str, limit: int = 500) -> List[List[Any]]:
        """GET /fapi/v1/klines"""
        params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
        return await self._request("GET", "/fapi/v1/klines", params=params)

    async def get_tickers(self, symbol: str | None = None) -> List[Dict[str, Any]]:
        """GET /fapi/v1/ticker/bookTicker"""
        params = {}
        if symbol:
            params["symbol"] = symbol.upper()
        res = await self._request("GET", "/fapi/v1/ticker/bookTicker", params=params)
        return [res] if isinstance(res, dict) else res

    async def get_exchange_info(self, symbol: str | None = None) -> Dict[str, Any]:
        """GET /fapi/v1/exchangeInfo"""
        params = {}
        if symbol:
            params["symbol"] = symbol.upper()
        return await self._request("GET", "/fapi/v1/exchangeInfo", params=params)

    async def get_premium_index(
        self, symbol: str | None = None
    ) -> Dict[str, Any] | List[Dict[str, Any]]:
        """GET /fapi/v1/premiumIndex"""
        params = {}
        if symbol:
            params["symbol"] = symbol.upper()
        return await self._request("GET", "/fapi/v1/premiumIndex", params=params)

    async def get_open_interest(self, symbol: str) -> Dict[str, Any]:
        """GET /fapi/v1/openInterest"""
        return await self._request(
            "GET", "/fapi/v1/openInterest", params={"symbol": symbol.upper()}
        )

    async def cancel_all_open_orders(self, symbol: str) -> Any:
        """DELETE /fapi/v1/allOpenOrders"""
        return await self._request(
            "DELETE", "/fapi/v1/allOpenOrders", params={"symbol": symbol.upper()}, signed=True
        )

    async def create_market_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        reduce_only: bool = False,
        new_client_order_id: str | None = None,
    ) -> Any:
        """POST /fapi/v1/order"""
        params = {
            "symbol": symbol.upper(),
            "side": side.upper(),
            "type": "MARKET",
            "quantity": quantity,
            "reduceOnly": "true" if reduce_only else "false",
        }
        if new_client_order_id:
            params["newClientOrderId"] = str(new_client_order_id)
        return await self._request("POST", "/fapi/v1/order", params=params, signed=True)

    async def create_limit_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        time_in_force: str = "GTC",
        reduce_only: bool = False,
        new_client_order_id: str | None = None,
    ) -> Any:
        """POST /fapi/v1/order"""
        params = {
            "symbol": symbol.upper(),
            "side": side.upper(),
            "type": "LIMIT",
            "quantity": quantity,
            "price": price,
            "timeInForce": time_in_force.upper(),
            "reduceOnly": "true" if reduce_only else "false",
        }
        if new_client_order_id:
            params["newClientOrderId"] = str(new_client_order_id)
        return await self._request("POST", "/fapi/v1/order", params=params, signed=True)
