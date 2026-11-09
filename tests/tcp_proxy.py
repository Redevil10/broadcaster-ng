"""A small TCP proxy for simulating network failures in tests."""

from __future__ import annotations

import asyncio
import contextlib


class TcpProxy:
    """Forwards 127.0.0.1:<port> to a target, with failure controls.

    - `reset()`: close every proxied connection (like a server restart).
    - `refuse()` / `accept()`: stop / resume accepting new connections, so new
      connections are refused (like a server that is down).
    - `mute()` / `unmute()`: keep all sockets open but silently drop traffic in
      both directions (a half-open link, or a server that accepts connections
      but never answers).
    """

    def __init__(self, target_host: str, target_port: int) -> None:
        self.target_host = target_host
        self.target_port = target_port
        self.port = 0
        self.muted = False
        self.connections_accepted = 0
        self._server: asyncio.base_events.Server | None = None
        self._writers: set[asyncio.StreamWriter] = set()
        self._tasks: set[asyncio.Task[None]] = set()

    async def start(self) -> None:
        await self.accept()

    async def accept(self) -> None:
        if self._server is None:
            self._server = await asyncio.start_server(self._handle, "127.0.0.1", self.port, reuse_address=True)
            self.port = self._server.sockets[0].getsockname()[1]

    async def refuse(self) -> None:
        if self._server is not None:
            # close() stops listening at once. Do not await wait_closed(): since
            # Python 3.12 it also waits for active connections to close.
            self._server.close()
            self._server = None

    def mute(self) -> None:
        self.muted = True

    def unmute(self) -> None:
        self.muted = False

    async def reset(self) -> None:
        for writer in list(self._writers):
            writer.close()
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._writers.clear()

    async def stop(self) -> None:
        await self.refuse()
        await self.reset()

    async def _handle(self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter) -> None:
        self.connections_accepted += 1
        try:
            server_reader, server_writer = await asyncio.open_connection(self.target_host, self.target_port)
        except OSError:
            client_writer.close()
            return
        self._writers.update({client_writer, server_writer})
        for reader, writer in ((client_reader, server_writer), (server_reader, client_writer)):
            task = asyncio.create_task(self._pump(reader, writer))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _pump(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while data := await reader.read(65536):
                if self.muted:
                    continue  # drop it: the peer sees silence, not an error
                writer.write(data)
                await writer.drain()
        except (OSError, asyncio.CancelledError):
            pass
        finally:
            with contextlib.suppress(Exception):
                writer.close()
            self._writers.discard(writer)
