from __future__ import annotations

import asyncio
import json
import logging
import time
from types import SimpleNamespace
from typing import Any, Callable, List, Optional

try:
    import websockets  # type: ignore[import-not-found]
except ModuleNotFoundError:
    def _missing_connect(*_args: Any, **_kwargs: Any) -> Any:
        raise ModuleNotFoundError(
            "Optional dependency 'websockets' is required for live WebSocket connectivity."
        )

    websockets = SimpleNamespace(  # type: ignore[assignment]
        connect=_missing_connect,
        WebSocketClientProtocol=object,
    )

_LOG = logging.getLogger(__name__)


class _SubscriptionRejected(RuntimeError):
    """Raised when the venue rejects a websocket subscription request."""


class BybitWebSocketClient:
    """Async WebSocket client for Bybit V5 Derivatives."""

    BASE_URL = "wss://stream.bybit.com/v5/public/linear"

    def __init__(
        self,
        streams: List[str],
        on_message: Callable[[dict], None],
        on_reconnect_exhausted: Optional[Callable[[], None]] = None,
    ):
        self.streams = streams
        self.on_message = on_message
        self.on_reconnect_exhausted = on_reconnect_exhausted
        self._connection: Any | None = None
        self._running = False
        self._task: asyncio.Task | None = None

    async def connect(self):
        """Establish connection and start listening."""
        self._running = True
        self._task = asyncio.create_task(self._listen())

    async def disconnect(self):
        """Close connection."""
        self._running = False
        if self._connection:
            await self._connection.close()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    _PING_INTERVAL_SECONDS = 18.0  # Bybit drops connections after ~20s without ping

    async def _heartbeat(self, ws: Any) -> None:
        """Send periodic pings to keep the Bybit connection alive."""
        while self._running:
            await asyncio.sleep(self._PING_INTERVAL_SECONDS)
            if not self._running:
                break
            try:
                await ws.send(json.dumps({"op": "ping"}))
            except Exception as e:
                _LOG.debug(f"Bybit WS ping failed: {e}")
                break

    async def _listen(self):
        url = self.BASE_URL
        _LOG.info(f"Connecting to Bybit WS: {url}")

        import random

        max_retries = 5
        base_delay = 1.0
        retry_count = 0
        last_connect_time = 0.0

        while self._running:
            try:
                async with websockets.connect(url) as ws:
                    self._connection = ws
                    _LOG.info("Connected to Bybit WS.")

                    # Subscribe to streams
                    subscribe_msg = {
                        "op": "subscribe",
                        "args": self.streams
                    }
                    await ws.send(json.dumps(subscribe_msg))

                    # Reset retry count only if connection was stable for > 60s
                    if time.time() - last_connect_time > 60.0:
                        retry_count = 0
                    last_connect_time = time.time()

                    ping_task = asyncio.create_task(self._heartbeat(ws))
                    try:
                        async for message in ws:
                            if time.time() - last_connect_time > 60.0:
                                retry_count = 0

                            if not self._running:
                                break
                            try:
                                data = json.loads(message)

                                # Subscription confirmation
                                if data.get("op") == "subscribe":
                                    if not data.get("success", True):
                                        raise _SubscriptionRejected(
                                            str(data.get("ret_msg", "") or "subscription rejected")
                                        )
                                    continue

                                if data.get("op") in ("pong", "ping"):
                                    continue

                                if self.on_message:
                                    if asyncio.iscoroutinefunction(self.on_message):
                                        await self.on_message(data)
                                    else:
                                        self.on_message(data)
                            except _SubscriptionRejected:
                                raise
                            except Exception as e:
                                _LOG.error(f"Error processing Bybit WS message: {e}")
                    finally:
                        ping_task.cancel()
                        try:
                            await ping_task
                        except asyncio.CancelledError:
                            pass

            except Exception as e:
                if self._running:
                    if retry_count >= max_retries:
                        _LOG.error(f"Bybit WS max retries exhausted: {e}")
                        self._running = False
                        if self.on_reconnect_exhausted is not None:
                            if asyncio.iscoroutinefunction(self.on_reconnect_exhausted):
                                await self.on_reconnect_exhausted()
                            else:
                                self.on_reconnect_exhausted()
                        break
                    delay = base_delay * (2**retry_count) + random.uniform(0, 1)
                    _LOG.error(f"Bybit WS connection error: {e}. Reconnecting in {delay:.2f}s...")
                    await asyncio.sleep(delay)
                    retry_count += 1
