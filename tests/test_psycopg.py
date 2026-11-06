"""Postgres backend using psycopg 3 (postgres+psycopg:// URLs).

The same connection-loss scenarios as the asyncpg backend, against a real
Postgres behind a TCP proxy that can reset, refuse or mute connections.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import typing
from collections.abc import AsyncIterator, Iterator
from urllib.parse import urlsplit, urlunsplit

import psycopg
import psycopg.conninfo
import pytest
import pytest_asyncio

from broadcaster import Broadcast, Event
from broadcaster._base import Subscriber
from broadcaster.backends.psycopg import PsycopgBackend, _libpq_url

from .tcp_proxy import TcpProxy

POSTGRES_URL = os.environ.get(
    "BROADCASTER_TEST_POSTGRES_URL", "postgres://postgres:postgres@localhost:5432/broadcaster"
)
HEARTBEAT_AFTER = 0.2


@pytest.fixture(autouse=True)
def fast_timings(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for name, value in {
        "_CONNECT_TIMEOUT": 1.0,
        "_COMMAND_TIMEOUT": 0.5,
        "_READ_TIMEOUT": 0.05,
        "_HEARTBEAT_AFTER": HEARTBEAT_AFTER,
        "_SUBSCRIBE_TIMEOUT": 3.0,
        "_RECONNECT_BACKOFF_INITIAL": 0.05,
        "_RECONNECT_BACKOFF_MAX": 0.2,
        "_STOP_TIMEOUT": 1.0,
    }.items():
        monkeypatch.setattr(PsycopgBackend, name, value)
    yield


def psycopg_url(netloc: str | None = None, query: str = "") -> str:
    parts = urlsplit(POSTGRES_URL)
    return urlunsplit(parts._replace(scheme="postgres+psycopg", netloc=netloc or parts.netloc, query=query))


@pytest_asyncio.fixture
async def proxy() -> AsyncIterator[TcpProxy]:
    target = urlsplit(POSTGRES_URL)
    proxy = TcpProxy(target.hostname or "localhost", target.port or 5432)
    await proxy.start()
    yield proxy
    await proxy.stop()


def proxied_url(proxy: TcpProxy, query: str = "") -> str:
    netloc = urlsplit(POSTGRES_URL).netloc.rsplit("@", 1)[0] + f"@127.0.0.1:{proxy.port}"
    return psycopg_url(netloc, query)


async def publish_until_received(
    broadcast: Broadcast, subscriber: Subscriber, channel: str = "chatroom", timeout: float = 10.0
) -> Event:
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
async def test_publish_and_subscribe() -> None:
    async with Broadcast(psycopg_url()) as broadcast:
        assert isinstance(broadcast._backend, PsycopgBackend)
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            assert await asyncio.wait_for(subscriber.get(), 5) == Event("chatroom", "hello")


@pytest.mark.asyncio
async def test_channel_names_are_quoted() -> None:
    channel = 'Mixed Case "quoted" room'
    async with Broadcast(psycopg_url()) as broadcast:
        async with broadcast.subscribe(channel) as subscriber:
            await broadcast.publish(channel, "hello")
            assert (await asyncio.wait_for(subscriber.get(), 5)).channel == channel


@pytest.mark.asyncio
async def test_reconnects_after_backend_terminated(caplog: pytest.LogCaptureFixture) -> None:
    calls: list[tuple[list[str], float]] = []
    admin = await psycopg.AsyncConnection.connect(urlunsplit(urlsplit(POSTGRES_URL)._replace(scheme="postgresql")))
    try:
        with caplog.at_level(logging.WARNING, logger="broadcaster"):
            async with Broadcast(psycopg_url(), on_reconnect=lambda c, d: calls.append((c, d))) as broadcast:
                async with broadcast.subscribe("chatroom") as subscriber:
                    await publish_until_received(broadcast, subscriber)
                    await admin.execute(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = %s",
                        (PsycopgBackend._APPLICATION_NAME,),
                    )
                    await publish_until_received(broadcast, subscriber)
    finally:
        await admin.close()
    assert "Reconnected to Postgres" in caplog.text
    assert [channels for channels, _ in calls] == [["chatroom"]]


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
    assert caplog.text.count("Could not reconnect to Postgres") == 1
    assert "Reconnected to Postgres after" in caplog.text


@pytest.mark.asyncio
async def test_half_open_connection_is_detected(proxy: TcpProxy, caplog: pytest.LogCaptureFixture) -> None:
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
                await asyncio.sleep(HEARTBEAT_AFTER * 8)
                assert proxy.connections_accepted == accepted
                await publish_until_received(broadcast, subscriber)
    assert "heartbeat failed" not in caplog.text
    assert "Lost connection" not in caplog.text


@pytest.mark.asyncio
async def test_concurrent_subscribes_to_many_channels() -> None:
    channels = [f"channel_{i}" for i in range(30)]
    async with Broadcast(psycopg_url()) as broadcast:

        async def subscribe_and_receive(channel: str) -> Event:
            async with broadcast.subscribe(channel) as subscriber:
                return await publish_until_received(broadcast, subscriber, channel)

        events = await asyncio.wait_for(asyncio.gather(*(subscribe_and_receive(c) for c in channels)), 20)
    assert sorted(e.channel for e in events) == sorted(channels)


@pytest.mark.asyncio
async def test_subscribe_during_long_outage_raises(proxy: TcpProxy, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(PsycopgBackend, "_SUBSCRIBE_TIMEOUT", 0.5)
    async with Broadcast(proxied_url(proxy)) as broadcast:
        async with broadcast.subscribe("other"):
            await proxy.refuse()
            await proxy.reset()
            await asyncio.sleep(0.2)
            with pytest.raises(ConnectionError, match="Postgres is unavailable"):
                async with broadcast.subscribe("chatroom"):
                    pass  # pragma: no cover
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
        started = time.monotonic()
        await asyncio.wait_for(broadcast.disconnect(), 5)
    assert time.monotonic() - started < 3


@pytest.mark.asyncio
async def test_connect_fails_fast_when_postgres_is_unreachable(proxy: TcpProxy) -> None:
    await proxy.refuse()
    with pytest.raises(Exception):
        await asyncio.wait_for(Broadcast(proxied_url(proxy)).connect(), 3)


@pytest.mark.asyncio
async def test_libpq_keepalive_params_are_accepted(proxy: TcpProxy) -> None:
    url = proxied_url(proxy, "keepalives=1&keepalives_idle=30&keepalives_interval=5&keepalives_count=3")
    async with Broadcast(url) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await publish_until_received(broadcast, subscriber)


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


@pytest.mark.asyncio
async def test_leaving_subscription_on_dead_connection(proxy: TcpProxy, caplog: pytest.LogCaptureFixture) -> None:
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
async def test_publish_recovers_after_publish_connection_breaks(proxy: TcpProxy) -> None:
    async with Broadcast(proxied_url(proxy)) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await publish_until_received(broadcast, subscriber)
            proxy.mute()
            with pytest.raises(Exception):
                await broadcast.publish("chatroom", "lost")
            proxy.unmute()
            await proxy.reset()
            await publish_until_received(broadcast, subscriber)


@pytest.mark.asyncio
async def test_reader_crash_is_logged_and_restarted(
    proxy: TcpProxy, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(PsycopgBackend, "_READER_RESTART_DELAY", 0.05)
    original_run = PsycopgBackend._run
    crashes: list[int] = []

    async def crash_once(self: PsycopgBackend) -> None:
        if not crashes:
            crashes.append(1)
            raise RuntimeError("bug in reader")
        await original_run(self)

    monkeypatch.setattr(PsycopgBackend, "_run", crash_once)
    with caplog.at_level(logging.ERROR, logger="broadcaster"):
        async with Broadcast(proxied_url(proxy)) as broadcast:
            async with broadcast.subscribe("chatroom") as subscriber:
                await publish_until_received(broadcast, subscriber)
    assert "reader stopped unexpectedly" in caplog.text


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("postgres+psycopg://u:p@h:5432/db?sslmode=disable", "postgresql://u:p@h:5432/db?sslmode=disable"),
        # The Unix-socket form: the empty host must keep its "//".
        ("postgresql+psycopg:///db?host=/var/run/postgresql", "postgresql:///db?host=/var/run/postgresql"),
    ],
)
def test_libpq_url(url: str, expected: str) -> None:
    assert _libpq_url(url) == expected
    assert psycopg.conninfo.conninfo_to_dict(_libpq_url(url))["dbname"] == "db"


@pytest.mark.asyncio
async def test_notification_arriving_during_another_command_is_kept() -> None:
    # The LISTEN connection also runs LISTEN/UNLISTEN and the heartbeat. A
    # notification that arrives meanwhile must still reach the reader (psycopg
    # buffers it since 3.2.4; older versions dropped it).
    backend = PsycopgBackend(psycopg_url())
    async with Broadcast(backend=backend) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            conn = backend._conn
            assert conn is not None
            async with backend._lock:  # keep other commands off the connection
                await asyncio.sleep(0.2)  # let the reader leave notifies()
                await asyncio.gather(conn.execute("SELECT pg_sleep(0.3)"), broadcast.publish("chatroom", "during"))
            assert (await asyncio.wait_for(subscriber.get(), 2)).message == "during"


@pytest.mark.asyncio
async def test_cancelled_subscribe_keeps_the_shared_connection() -> None:
    # Cancelling one caller's LISTEN (e.g. a websocket handler cancelled by
    # its task group) used to close the LISTEN connection for every channel.
    backend = PsycopgBackend(psycopg_url())
    async with Broadcast(backend=backend) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            conn = backend._conn
            assert conn is not None
            busy = asyncio.create_task(conn.execute("SELECT pg_sleep(0.3)"))  # LISTEN queues behind it
            await asyncio.sleep(0.05)

            async def join() -> None:
                async with broadcast.subscribe("other"):
                    pass  # pragma: no cover

            task = asyncio.create_task(join())
            await asyncio.sleep(0.05)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await busy
            assert backend._conn is conn and not conn.closed
            assert "other" not in backend._channels
            await broadcast.publish("chatroom", "still here")
            assert (await asyncio.wait_for(subscriber.get(), 2)).message == "still here"


@pytest.mark.asyncio
async def test_listen_rejected_by_server_does_not_drop_the_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    original = PsycopgBackend._execute

    async def execute(conn: typing.Any, command: str, channel: str) -> None:
        if channel == "bad":
            await conn.execute("LISTEN")  # syntax error
        await original(conn, command, channel)

    monkeypatch.setattr(PsycopgBackend, "_execute", staticmethod(execute))
    backend = PsycopgBackend(psycopg_url())
    async with Broadcast(backend=backend) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            conn = backend._conn
            with pytest.raises(psycopg.errors.SyntaxError):
                async with broadcast.subscribe("bad"):
                    pass  # pragma: no cover
            assert backend._conn is conn
            assert "bad" not in backend._channels
            await broadcast.publish("chatroom", "still here")
            assert (await asyncio.wait_for(subscriber.get(), 2)).message == "still here"


@pytest.mark.asyncio
async def test_invalid_channel_name() -> None:
    async with Broadcast(psycopg_url()) as broadcast:
        with pytest.raises(ValueError, match="invalid Postgres channel name"):
            async with broadcast.subscribe(""):
                pass  # pragma: no cover


@pytest.mark.asyncio
async def test_application_name_and_connect_timeout_in_url_are_kept() -> None:
    backend = PsycopgBackend(psycopg_url(query="application_name=myapp&connect_timeout=15"))
    async with Broadcast(backend=backend):
        conn = backend._conn
        assert conn is not None
        assert conn.info.get_parameters()["connect_timeout"] == "15"
        async with backend._lock:
            cur = await conn.execute("SHOW application_name")
            assert await cur.fetchone() == ("myapp",)


@pytest.mark.asyncio
async def test_connection_lost_right_after_opening_backs_off(monkeypatch: pytest.MonkeyPatch) -> None:
    # E.g. a load balancer that accepts connections and drops them at once:
    # without backoff the reader reconnected in a tight loop.
    opens = 0
    original_open = PsycopgBackend._open

    async def counting_open(self: PsycopgBackend) -> None:
        nonlocal opens
        opens += 1
        await original_open(self)

    async def failing_heartbeat(self: PsycopgBackend, conn: typing.Any) -> None:
        async with self._lock:
            await self._drop(conn)

    monkeypatch.setattr(PsycopgBackend, "_open", counting_open)
    async with Broadcast(psycopg_url()) as broadcast:
        async with broadcast.subscribe("chatroom"):
            monkeypatch.setattr(PsycopgBackend, "_HEARTBEAT_AFTER", 0.0)
            monkeypatch.setattr(PsycopgBackend, "_heartbeat", failing_heartbeat)
            opens = 0
            await asyncio.sleep(1.0)
    # Backoff 0.05 s doubling to 0.2 s: about 6 attempts, not hundreds.
    assert 1 <= opens <= 10


@pytest.mark.asyncio
async def test_cancelled_listen_is_undone_on_the_server() -> None:
    backend = PsycopgBackend(psycopg_url())
    async with Broadcast(backend=backend) as broadcast:
        async with broadcast.subscribe("chatroom"):
            conn = backend._conn
            assert conn is not None
            busy = asyncio.create_task(conn.execute("SELECT pg_sleep(0.3)"))  # LISTEN queues behind it
            await asyncio.sleep(0.05)

            async def join() -> None:
                async with broadcast.subscribe("orphan"):
                    pass  # pragma: no cover

            task = asyncio.create_task(join())
            await asyncio.sleep(0.05)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await busy
            await asyncio.sleep(0.2)  # the background LISTEN, then the UNLISTEN
            async with backend._lock:
                cur = await conn.execute("SELECT pg_listening_channels()")
                assert [row[0] for row in await cur.fetchall()] == ["chatroom"]
