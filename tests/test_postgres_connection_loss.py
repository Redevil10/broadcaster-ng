"""Postgres backend: reconnect, heartbeat, serialised commands, keepalive.

Uses a real Postgres behind a TCP proxy that can reset, refuse or mute connections.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import time
import typing
import uuid
from collections.abc import AsyncIterator, Iterator
from urllib.parse import urlsplit, urlunsplit

import asyncpg
import pytest
import pytest_asyncio

from broadcaster import Broadcast, Event
from broadcaster._base import Subscriber
from broadcaster.backends.postgres import PostgresBackend, _apply_keepalive, _split_keepalive_params

from .tcp_proxy import TcpProxy

POSTGRES_URL = os.environ.get(
    "BROADCASTER_TEST_POSTGRES_URL", "postgres://postgres:postgres@localhost:5432/broadcaster"
)
HEARTBEAT_AFTER = 0.2


@pytest.fixture(autouse=True)
def fast_timings(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for name, value in {
        "_CONNECT_TIMEOUT": 0.5,
        "_COMMAND_TIMEOUT": 0.5,
        "_HEARTBEAT_AFTER": HEARTBEAT_AFTER,
        "_SUBSCRIBE_TIMEOUT": 3.0,
        "_RECONNECT_BACKOFF_INITIAL": 0.05,
        "_RECONNECT_BACKOFF_MAX": 0.2,
        "_STOP_TIMEOUT": 1.0,
    }.items():
        monkeypatch.setattr(PostgresBackend, name, value)
    yield


@pytest_asyncio.fixture
async def proxy() -> AsyncIterator[TcpProxy]:
    target = urlsplit(POSTGRES_URL)
    proxy = TcpProxy(target.hostname or "localhost", target.port or 5432)
    await proxy.start()
    yield proxy
    await proxy.stop()


def proxied_url(proxy: TcpProxy, query: str = "") -> str:
    parts = urlsplit(POSTGRES_URL)
    netloc = parts.netloc.rsplit("@", 1)[0] + f"@127.0.0.1:{proxy.port}"
    return urlunsplit(parts._replace(netloc=netloc, query=query))


def listener_backend(broadcast: Broadcast) -> PostgresBackend:
    backend = broadcast._backend
    assert isinstance(backend, PostgresBackend)
    return backend


async def publish_until_received(
    broadcast: Broadcast, subscriber: Subscriber, channel: str = "chatroom", timeout: float = 10.0
) -> Event:
    """Publish repeatedly until the subscriber gets a message (NOTIFY is not stored,
    so notifications sent while the listener reconnects are lost)."""
    deadline = time.monotonic() + timeout
    n = 0
    while time.monotonic() < deadline:
        n += 1
        try:
            await broadcast.publish(channel, f"probe-{n}")
        except Exception:
            pass
        try:
            return await asyncio.wait_for(subscriber.get(), 0.1)
        except asyncio.TimeoutError:
            continue
    raise AssertionError(f"no message received within {timeout} s")


@pytest.mark.asyncio
async def test_reconnects_after_backend_terminated(caplog: pytest.LogCaptureFixture) -> None:
    # Fix #6: pg_terminate_backend() on the listener connection.
    admin = await asyncpg.connect(POSTGRES_URL)
    try:
        with caplog.at_level(logging.WARNING, logger="broadcaster"):
            async with Broadcast(POSTGRES_URL) as broadcast:
                async with broadcast.subscribe("chatroom") as subscriber:
                    await publish_until_received(broadcast, subscriber)
                    terminated = await admin.fetchval(
                        "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE application_name = $1",
                        PostgresBackend._APPLICATION_NAME,
                    )
                    assert terminated >= 1
                    await publish_until_received(broadcast, subscriber)
    finally:
        await admin.close()
    assert "Reconnected to Postgres" in caplog.text


@pytest.mark.asyncio
async def test_reconnects_after_connection_reset(proxy: TcpProxy) -> None:
    async with Broadcast(proxied_url(proxy)) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await publish_until_received(broadcast, subscriber)
            accepted = proxy.connections_accepted
            await proxy.reset()
            await publish_until_received(broadcast, subscriber)
            assert proxy.connections_accepted > accepted


@pytest.mark.asyncio
async def test_reconnects_after_outage(proxy: TcpProxy, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="broadcaster"):
        async with Broadcast(proxied_url(proxy)) as broadcast:
            async with broadcast.subscribe("chatroom") as subscriber:
                await proxy.refuse()
                await proxy.reset()
                await asyncio.sleep(0.6)
                await proxy.accept()
                await publish_until_received(broadcast, subscriber)
    assert "Could not reconnect to Postgres (attempt 1)" in caplog.text
    assert caplog.text.count("Could not reconnect to Postgres") == 1
    assert "Reconnected to Postgres after" in caplog.text


@pytest.mark.asyncio
async def test_half_open_connection_is_detected(proxy: TcpProxy, caplog: pytest.LogCaptureFixture) -> None:
    # Fix #7: nothing gets through but both sockets stay open.
    with caplog.at_level(logging.WARNING, logger="broadcaster"):
        async with Broadcast(proxied_url(proxy)) as broadcast:
            async with broadcast.subscribe("chatroom") as subscriber:
                await publish_until_received(broadcast, subscriber)
                proxy.mute()
                await asyncio.sleep(HEARTBEAT_AFTER + 1.0)
                assert "Postgres heartbeat failed" in caplog.text
                proxy.unmute()
                await publish_until_received(broadcast, subscriber)


@pytest.mark.asyncio
async def test_idle_healthy_connection_is_kept(proxy: TcpProxy, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="broadcaster"):
        async with Broadcast(proxied_url(proxy)) as broadcast:
            async with broadcast.subscribe("chatroom") as subscriber:
                await publish_until_received(broadcast, subscriber)
                accepted = proxy.connections_accepted
                await asyncio.sleep(HEARTBEAT_AFTER * 8)  # several heartbeats
                assert proxy.connections_accepted == accepted
                await publish_until_received(broadcast, subscriber)
    assert "heartbeat failed" not in caplog.text
    assert "Lost connection" not in caplog.text


@pytest.mark.asyncio
async def test_concurrent_subscribes_to_many_channels() -> None:
    # Fix #8 (encode#22): concurrent LISTENs on one asyncpg connection failed with
    # "cannot perform operation: another operation is in progress".
    channels = [f"channel_{i}" for i in range(30)]
    async with Broadcast(POSTGRES_URL) as broadcast:

        async def subscribe_and_receive(channel: str) -> Event:
            async with broadcast.subscribe(channel) as subscriber:
                return await publish_until_received(broadcast, subscriber, channel)

        events = await asyncio.wait_for(asyncio.gather(*(subscribe_and_receive(c) for c in channels)), 20)
    assert sorted(e.channel for e in events) == sorted(channels)


@pytest.mark.asyncio
async def test_subscribes_during_half_open_link_are_bounded(proxy: TcpProxy) -> None:
    # Fix #8: a LISTEN on a half-open connection must not hold the lock forever;
    # queued subscribes complete once the link is back.
    async with Broadcast(proxied_url(proxy)) as broadcast:
        async with broadcast.subscribe("chatroom"):
            proxy.mute()

            async def subscribe_and_receive(channel: str) -> Event:
                async with broadcast.subscribe(channel) as subscriber:
                    return await publish_until_received(broadcast, subscriber, channel)

            tasks = [asyncio.create_task(subscribe_and_receive(f"room_{i}")) for i in range(5)]
            await asyncio.sleep(1.0)
            proxy.unmute()
            events = await asyncio.wait_for(asyncio.gather(*tasks), 10)
            assert sorted(e.channel for e in events) == [f"room_{i}" for i in range(5)]


@pytest.mark.asyncio
async def test_publish_works_while_listener_connection_is_busy() -> None:
    # Fix #9: publish used the LISTEN connection, so it failed (or queued) while
    # that connection was busy or stuck.
    async with Broadcast(POSTGRES_URL) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            backend = listener_backend(broadcast)
            conn = backend._conn
            assert conn is not None

            async def slow_listener_command() -> None:
                # What a slow or stuck LISTEN looks like: the listener lock is
                # held and its connection is busy.
                async with backend._lock:
                    await conn.fetchval("SELECT pg_sleep(0.8)", timeout=5)

            busy = asyncio.create_task(slow_listener_command())
            await asyncio.sleep(0.1)
            await asyncio.wait_for(broadcast.publish("chatroom", "hello"), 0.5)
            assert (await asyncio.wait_for(subscriber.get(), 2)).message == "hello"
            await busy


@pytest.mark.asyncio
async def test_subscribe_during_long_outage_raises(proxy: TcpProxy, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(PostgresBackend, "_SUBSCRIBE_TIMEOUT", 0.5)
    async with Broadcast(proxied_url(proxy)) as broadcast:
        async with broadcast.subscribe("other"):
            await proxy.refuse()
            await proxy.reset()
            await asyncio.sleep(0.1)
            with pytest.raises(ConnectionError, match="Postgres is unavailable"):
                async with broadcast.subscribe("chatroom"):
                    pass  # pragma: no cover
            assert "chatroom" not in listener_backend(broadcast)._channels
            await proxy.accept()
            async with broadcast.subscribe("chatroom") as subscriber:
                await publish_until_received(broadcast, subscriber)


@pytest.mark.asyncio
async def test_disconnect_during_outage_is_prompt(proxy: TcpProxy) -> None:
    broadcast = Broadcast(proxied_url(proxy))
    await broadcast.connect()
    async with broadcast.subscribe("chatroom"):
        await proxy.refuse()
        await proxy.reset()
        await asyncio.sleep(0.3)
    await asyncio.wait_for(broadcast.disconnect(), 3)


@pytest.mark.asyncio
async def test_disconnect_while_half_open_is_prompt(proxy: TcpProxy) -> None:
    broadcast = Broadcast(proxied_url(proxy))
    await broadcast.connect()
    async with broadcast.subscribe("chatroom"):
        proxy.mute()
        await asyncio.wait_for(broadcast.disconnect(), 3)


@pytest.mark.asyncio
async def test_connect_fails_fast_when_postgres_is_unreachable(proxy: TcpProxy) -> None:
    await proxy.refuse()
    with pytest.raises(Exception):
        await asyncio.wait_for(Broadcast(proxied_url(proxy)).connect(), 3)


def _keepalive_state(broadcast: Broadcast) -> int:
    conn = listener_backend(broadcast)._conn
    assert conn is not None
    sock = conn._transport.get_extra_info("socket")
    # Non-zero means on (macOS returns the flag value, 8, rather than 1).
    return int(sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) != 0)


@pytest.mark.asyncio
async def test_libpq_keepalive_params_are_accepted_and_applied(proxy: TcpProxy) -> None:
    # asyncpg would send these to the server as settings and fail to connect.
    url = proxied_url(proxy, "keepalives=1&keepalives_idle=30&keepalives_interval=5&keepalives_count=3")
    async with Broadcast(url) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await publish_until_received(broadcast, subscriber)
            assert _keepalive_state(broadcast) == 1
            conn = listener_backend(broadcast)._conn
            assert conn is not None
            sock = conn._transport.get_extra_info("socket")
            idle_option = getattr(socket, "TCP_KEEPIDLE", None) or getattr(socket, "TCP_KEEPALIVE")
            assert sock.getsockopt(socket.IPPROTO_TCP, idle_option) == 30
            assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT) == 3


@pytest.mark.asyncio
async def test_keepalive_is_on_by_default_and_can_be_disabled(proxy: TcpProxy) -> None:
    async with Broadcast(proxied_url(proxy)) as broadcast:
        assert _keepalive_state(broadcast) == 1
    async with Broadcast(proxied_url(proxy, "keepalives=0")) as broadcast:
        assert _keepalive_state(broadcast) == 0


def test_split_keepalive_params_keeps_other_params() -> None:
    url, options = _split_keepalive_params(
        "postgres://u:p@h:5432/db?sslmode=disable&keepalives_idle=7&application_name=x"
    )
    assert url == "postgres://u:p@h:5432/db?sslmode=disable&application_name=x"
    assert options == {"keepalives_idle": 7}


@pytest.mark.asyncio
async def test_disconnect_while_reconnecting_to_unresponsive_server_is_prompt(proxy: TcpProxy) -> None:
    # Fix #9a: the reader is stuck in a connect handshake that never completes.
    broadcast = Broadcast(proxied_url(proxy))
    await broadcast.connect()
    async with broadcast.subscribe("chatroom"):
        proxy.mute()
        await proxy.reset()
        await asyncio.sleep(0.1)  # reader is now reconnecting into the muted proxy
        started = time.monotonic()
        await asyncio.wait_for(broadcast.disconnect(), 3)
        assert time.monotonic() - started < 1.0


@pytest.mark.asyncio
async def test_disconnect_fails_pending_subscribe_promptly(proxy: TcpProxy) -> None:
    # Fix #9a: a subscribe() waiting for the reconnect must not wait out its
    # full timeout once the backend is disconnecting.
    broadcast = Broadcast(proxied_url(proxy))
    await broadcast.connect()
    async with broadcast.subscribe("other"):
        await proxy.refuse()
        await proxy.reset()
        await asyncio.sleep(0.1)

        async def subscribe() -> None:
            async with broadcast.subscribe("chatroom"):
                pass  # pragma: no cover

        pending = asyncio.create_task(subscribe())
        await asyncio.sleep(0.2)
        assert not pending.done()
        started = time.monotonic()
        await asyncio.wait_for(broadcast._backend.disconnect(), 3)
        with pytest.raises(ConnectionError):
            await asyncio.wait_for(pending, 1)
        assert time.monotonic() - started < 1.5


@pytest.mark.asyncio
async def test_leaving_subscription_on_dead_connection(proxy: TcpProxy, caplog: pytest.LogCaptureFixture) -> None:
    # UNSUBSCRIBE on a dead connection times out: leaving must not raise or hang,
    # and other subscribers recover after the reconnect.
    with caplog.at_level(logging.WARNING, logger="broadcaster"):
        async with Broadcast(proxied_url(proxy)) as broadcast:
            async with broadcast.subscribe("other") as other:
                await publish_until_received(broadcast, other, "other")
                async with broadcast.subscribe("chatroom"):
                    proxy.mute()
                    started = time.monotonic()
                assert time.monotonic() - started < 2.0
                assert "while unsubscribing from 'chatroom'" in caplog.text
                proxy.unmute()
                await publish_until_received(broadcast, other, "other")


@pytest.mark.asyncio
async def test_reader_crash_is_logged_and_restarted(
    proxy: TcpProxy, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # An unexpected error escaping the reader loop must not stop delivery silently.
    backend_cls = PostgresBackend
    monkeypatch.setattr(backend_cls, "_READER_RESTART_DELAY", 0.05)
    original_run = backend_cls._run
    crashes: list[int] = []

    async def crash_once(self: typing.Any) -> None:
        if not crashes:
            crashes.append(1)
            raise RuntimeError("bug in reader")
        await original_run(self)

    monkeypatch.setattr(backend_cls, "_run", crash_once)
    with caplog.at_level(logging.ERROR, logger="broadcaster"):
        async with Broadcast(proxied_url(proxy)) as broadcast:
            async with broadcast.subscribe("chatroom") as subscriber:
                await publish_until_received(broadcast, subscriber)
    assert "reader stopped unexpectedly" in caplog.text
    assert "bug in reader" in caplog.text


@pytest.mark.asyncio
async def test_disconnect_during_heartbeat_on_half_open_link_is_prompt(proxy: TcpProxy) -> None:
    # A heartbeat query is pending on a link that no longer delivers anything.
    # Cancelling a query makes the driver send a cancel request and wait for
    # it; disconnect() must not wait for that.
    broadcast = Broadcast(proxied_url(proxy))
    await broadcast.connect()
    async with broadcast.subscribe("chatroom"):
        proxy.mute()
        await asyncio.sleep(HEARTBEAT_AFTER + 0.1)  # the heartbeat is now in flight
        started = time.monotonic()
        await asyncio.wait_for(broadcast.disconnect(), 10)
    assert time.monotonic() - started < 2


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("postgresql:///db?host=/var/run/postgresql", "postgresql:///db?host=/var/run/postgresql"),
        ("postgresql:///db?keepalives=0&host=%2Ftmp", "postgresql:///db?host=%2Ftmp"),
        ("postgres://h/db?keepalives_idle=7", "postgres://h/db"),
    ],
)
def test_split_keepalive_params_leaves_the_rest_of_the_dsn_alone(url: str, expected: str) -> None:
    assert _split_keepalive_params(url)[0] == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("channel", ["", "a\x00b"])
async def test_invalid_channel_name(channel: str) -> None:
    async with Broadcast(POSTGRES_URL) as broadcast:
        with pytest.raises(ValueError, match="invalid Postgres channel name"):
            async with broadcast.subscribe(channel):
                pass  # pragma: no cover
        assert channel not in listener_backend(broadcast)._channels


@pytest.mark.asyncio
async def test_listen_rejected_by_server_does_not_drop_the_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    # A server error is the caller's problem, not a lost connection: the
    # LISTEN connection (and every other channel) must be left alone.
    async with Broadcast(POSTGRES_URL) as broadcast:
        backend = listener_backend(broadcast)
        async with broadcast.subscribe("chatroom") as subscriber:
            conn = backend._conn
            assert conn is not None
            original = type(conn).add_listener

            async def add_listener(self: typing.Any, channel: str, callback: typing.Any) -> None:
                if channel == "bad":
                    await self.execute("LISTEN")  # syntax error
                await original(self, channel, callback)

            monkeypatch.setattr(type(conn), "add_listener", add_listener)
            with pytest.raises(asyncpg.PostgresSyntaxError):
                async with broadcast.subscribe("bad"):
                    pass  # pragma: no cover
            assert backend._conn is conn
            assert "bad" not in backend._channels
            await broadcast.publish("chatroom", "still here")
            assert (await asyncio.wait_for(subscriber.get(), 2)).message == "still here"


@pytest.mark.asyncio
async def test_application_name_in_url_is_kept() -> None:
    name = f"myapp-{uuid.uuid4().hex[:8]}"
    url = urlunsplit(urlsplit(POSTGRES_URL)._replace(query=f"application_name={name}"))
    async with Broadcast(url) as broadcast:
        conn = listener_backend(broadcast)._conn
        assert conn is not None
        async with listener_backend(broadcast)._lock:
            assert await conn.fetchval("SHOW application_name") == name


def test_zero_keepalive_settings_mean_system_default() -> None:
    # libpq: 0 means "use the system default". Linux rejects 0 for these socket
    # options (EINVAL), so they must not be passed through.
    calls: list[tuple[int, int, int]] = []

    class FakeSocket:
        family = socket.AF_INET

        def setsockopt(self, level: int, option: int, value: int) -> None:
            if value == 0:
                raise OSError(22, "Invalid argument")
            calls.append((level, option, value))

    class FakeTransport:
        def get_extra_info(self, name: str) -> FakeSocket:
            return FakeSocket()

    class FakeConnection:
        _transport = FakeTransport()

    _apply_keepalive(
        typing.cast(asyncpg.Connection, FakeConnection()),
        {"keepalives_idle": 0, "keepalives_interval": 0, "keepalives_count": 0},
    )
    assert calls == [(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)]


@pytest.mark.asyncio
async def test_connection_lost_right_after_opening_backs_off(monkeypatch: pytest.MonkeyPatch) -> None:
    # E.g. a load balancer that accepts connections and drops them at once:
    # without backoff the reader reconnected in a tight loop.
    opens = 0
    original_open = PostgresBackend._open

    async def counting_open(self: PostgresBackend) -> None:
        nonlocal opens
        opens += 1
        await original_open(self)

    async def failing_heartbeat(self: PostgresBackend, conn: typing.Any) -> None:
        async with self._lock:
            self._drop(conn)

    monkeypatch.setattr(PostgresBackend, "_open", counting_open)
    async with Broadcast(POSTGRES_URL) as broadcast:
        async with broadcast.subscribe("chatroom"):
            monkeypatch.setattr(PostgresBackend, "_HEARTBEAT_AFTER", 0.0)
            monkeypatch.setattr(PostgresBackend, "_heartbeat", failing_heartbeat)
            opens = 0
            await asyncio.sleep(1.0)
    # Backoff 0.05 s doubling to 0.2 s: about 6 attempts, not hundreds.
    assert 1 <= opens <= 10


@pytest.mark.asyncio
async def test_cancelled_listen_is_undone_on_the_server(monkeypatch: pytest.MonkeyPatch) -> None:
    # Cancelled after LISTEN ran but before asyncpg registered the channel, so
    # remove_listener() alone would do nothing.
    async with Broadcast(POSTGRES_URL) as broadcast:
        backend = listener_backend(broadcast)
        async with broadcast.subscribe("chatroom"):
            conn = backend._conn
            assert conn is not None
            original = type(conn).add_listener
            listened = asyncio.Event()

            async def add_listener(self: typing.Any, channel: str, callback: typing.Any) -> None:
                if channel != "orphan":
                    await original(self, channel, callback)
                    return
                await self.execute('LISTEN "orphan"')
                listened.set()
                await asyncio.sleep(10)

            monkeypatch.setattr(type(conn), "add_listener", add_listener)

            async def join() -> None:
                async with broadcast.subscribe("orphan"):
                    pass  # pragma: no cover

            task = asyncio.create_task(join())
            await listened.wait()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await asyncio.sleep(0.2)
            async with backend._lock:
                channels = await conn.fetchval("SELECT array_agg(c) FROM pg_listening_channels() c")
            assert channels == ["chatroom"]
