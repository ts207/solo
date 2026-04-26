from __future__ import annotations

import asyncio
import unittest.mock as mock

from project.live.ingest.bybit_ws_client import BybitWebSocketClient


class _RejectingWebSocket:
    async def send(self, _message: str) -> None:
        return None

    async def close(self) -> None:
        return None

    def __aiter__(self):
        self._yielded = False
        return self

    async def __anext__(self) -> str:
        if getattr(self, "_yielded", False):
            raise StopAsyncIteration
        self._yielded = True
        return '{"op":"subscribe","success":false,"ret_msg":"invalid topic"}'


class _RejectingConnect:
    async def __aenter__(self):
        return _RejectingWebSocket()

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


def test_bybit_ws_subscription_rejection_exhausts_reconnects() -> None:
    exhausted_calls: list[bool] = []
    client = BybitWebSocketClient(
        streams=["tickers.BTCUSDT"],
        on_message=lambda _: None,
        on_reconnect_exhausted=lambda: exhausted_calls.append(True),
    )

    async def _run() -> None:
        async def _noop_sleep(_delay: float) -> None:
            return None

        with mock.patch(
            "project.live.ingest.bybit_ws_client.websockets.connect",
            side_effect=lambda *_args, **_kwargs: _RejectingConnect(),
        ), mock.patch("asyncio.sleep", side_effect=_noop_sleep):
            client._running = True
            await client._listen()

    asyncio.run(_run())
    assert exhausted_calls == [True]

