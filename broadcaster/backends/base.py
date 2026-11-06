from collections.abc import Callable
from typing import Any

from .._base import Event


class BroadcastBackend:
    # Set by Broadcast. A backend that reconnects by itself calls
    # `self._notify_reconnect(channels, downtime)` after re-subscribing, so the
    # application can learn that messages may have been lost.
    _reconnect_hook: Callable[[list[str], float], None] | None = None

    def _notify_reconnect(self, channels: list[str], downtime: float) -> None:
        if self._reconnect_hook is not None:
            self._reconnect_hook(channels, downtime)

    def __init__(self, url: str) -> None:
        raise NotImplementedError()

    async def connect(self) -> None:
        raise NotImplementedError()

    async def disconnect(self) -> None:
        raise NotImplementedError()

    async def subscribe(self, channel: str) -> None:
        raise NotImplementedError()

    async def unsubscribe(self, channel: str) -> None:
        raise NotImplementedError()

    async def publish(self, channel: str, message: Any) -> None:
        raise NotImplementedError()

    async def next_published(self) -> Event:
        raise NotImplementedError()
