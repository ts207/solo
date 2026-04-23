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


class BinanceWebSocketClient:
    """Async WebSocket client for Binance UM futures."""

    BASE_URL = "wss://fstream.binance.com/ws"

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

    async def _listen(self):
        if len(self.streams) > 1:
            url = f"wss://fstream.binance.com/stream?streams={'/'.join(self.streams)}"
        else:
            url = f"{self.BASE_URL}/{self.streams[0]}"
        _LOG.info(f"Connecting to Binance WS: {url}")

        import random

        max_retries = 5
        base_delay = 1.0
        retry_count = 0
        last_connect_time = 0.0

        while self._running:
            try:
                async with websockets.connect(url) as ws:
                    self._connection = ws
                    _LOG.info("Connected to Binance WS.")
                    
                    # Reset retry count only if connection was stable for > 60s
                    if time.time() - last_connect_time > 60.0:
                        retry_count = 0
                    last_connect_time = time.time()

                    async for message in ws:
                        # Reset on first successful message if stable? 
                        # Better to use time-based.
                        if time.time() - last_connect_time > 60.0:
                            retry_count = 0
                            
                        if not self._running:
                            break
                        try:
                            data = json.loads(message)
                            if self.on_message:
                                # if it's an async callback vs sync callback
                                if asyncio.iscoroutinefunction(self.on_message):
                                    await self.on_message(data)
                                else:
                                    self.on_message(data)
                        except Exception as e:
                            _LOG.error(f"Error processing WS message: {e}")
            except Exception as e:
                if self._running:
                    if retry_count >= max_retries:
                        _LOG.error(f"WS max retries exhausted: {e}")
                        self._running = False
                        if self.on_reconnect_exhausted is not None:
                            self.on_reconnect_exhausted()
                        break
                    delay = base_delay * (2**retry_count) + random.uniform(0, 1)
                    _LOG.error(f"WS connection error: {e}. Reconnecting in {delay:.2f}s...")
                    await asyncio.sleep(delay)
                    retry_count += 1
