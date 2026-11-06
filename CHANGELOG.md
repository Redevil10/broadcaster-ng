# Changelog

All notable changes to broadcaster-ng. It continues from
[encode/broadcaster](https://github.com/encode/broadcaster) 0.3.1; the public
API and the `broadcaster` import name are unchanged.

## 0.4.0 (2026-11-06)

### Fixed

- `subscribe()` no longer hides a backend error behind `KeyError` when the
  backend subscribe fails (encode/broadcaster#142).
- Two tasks subscribing to a new channel at the same time no longer lose one
  subscriber.
- The broadcast listener keeps running after a backend error instead of
  stopping delivery for every subscriber; errors are logged
  (encode/broadcaster#144).
- Redis pub/sub: reconnects and re-subscribes after connection loss; detects
  half-open connections with an idle-time `PING`; default socket timeouts so
  a reconnect to an unresponsive server cannot hang forever
  (encode/broadcaster#144, encode/broadcaster#147).
- Redis pub/sub: a subscriber never received messages with redis-py 8 on
  Python 3.10/3.11 (subscribe race).
- Redis pub/sub: works with clients created with `decode_responses=True`.
- Redis Streams: calling `XREAD` with no streams after the last unsubscribe
  (encode/broadcaster#149).
- Redis Streams: default socket timeouts (5 s, overridable in the URL), so a
  half-open connection no longer blocks reading forever; a stream unsubscribed
  while a message was in flight is no longer read again. Messages published
  during a connection loss are delivered after reconnecting.
- Postgres: reconnects and re-runs `LISTEN` after connection loss
  (encode/broadcaster#66); detects half-open connections with a heartbeat
  query; concurrent subscribes no longer fail with "another operation is in
  progress" (encode/broadcaster#22); publishing uses a separate connection.
  A `LISTEN` the server rejects is raised to the caller instead of
  disconnecting every channel; an empty channel name raises `ValueError`.
- A subscribe that fails or is cancelled (e.g. a websocket handler cancelled
  during an outage) no longer leaves the channel subscribed in the backend.
- Kafka: unsubscribing one channel no longer unsubscribes every channel
  (encode/broadcaster#65), and changing subscriptions no longer loses
  messages on the other channels.
- Kafka: subscribing to a topic that does not exist yet took up to 5
  minutes (until the consumer's next metadata refresh); now it is immediate,
  and it fails after 10 seconds with `ConnectionError` if the topic never
  appears instead of waiting forever.
- Kafka: a slow or missing topic no longer holds up subscription changes on
  other channels; the whole `subscribe()` is bounded by the 10 s timeout and
  a failed one restores the consumer's previous assignment. Partitions added
  to a subscribed topic are picked up within a minute.
- Leaving a subscription completes even when the task is cancelled during
  cleanup (anyio/Starlette cancel every await in it): the queue is
  deregistered and the backend unsubscribed. A cancelled subscribe is undone
  on the server.
- Concurrent first subscribers to a channel during an outage share one
  failed attempt instead of each waiting for its own.
- Redis pub/sub: `subscribe()` and reconnects wait for Redis to confirm
  `SUBSCRIBE`, so `on_reconnect` and messages published after `subscribe()`
  returns cannot race ahead of the subscription; no needless reconnect when
  the confirmation is read early.
- A server that accepts connections and drops them at once no longer causes a
  tight reconnect loop: such drops count as failed attempts, with backoff.
- Postgres: `keepalives_idle=0` (and the other keepalive settings) means the
  system default, as in libpq, instead of failing on Linux.
- `disconnect()` stops the listener reliably, even when a client library
  swallows the cancellation (redis-py 8 on Python 3.11).

### Changed

- Package name on PyPI is `broadcaster-ng`; the import name is still
  `broadcaster`. It cannot be installed together with `broadcaster`.
- Requires Python 3.10+ and, for Redis, redis-py 5 or newer.
- `subscribe()` during a Redis or Postgres outage waits up to 10 seconds for
  the reconnect, then raises `ConnectionError`.
- Leaving a subscription no longer raises if the backend unsubscribe fails; the
  error is logged instead, so it cannot mask an exception from your code.
- `disconnect()` no longer re-raises an earlier listener error (it was
  already logged when it happened).
- Postgres URLs accept libpq keepalive parameters (`keepalives`,
  `keepalives_idle`, `keepalives_interval`, `keepalives_count`); TCP keepalive
  is on by default. The listener connection is named `broadcaster-listener`
  unless the URL sets `application_name`.

### Added

- Postgres backend using psycopg 3, selected with `postgres+psycopg://` or
  `postgresql+psycopg://` URLs (`pip install "broadcaster-ng[psycopg]"`), with
  the same reconnect, heartbeat and timeout behaviour as the asyncpg backend.
  Plain `postgres://` still uses asyncpg.
- `subscribe(channel, max_queue_size=N)`: bound a subscriber's queue. When
  it is full the oldest message is dropped (logged, and counted in
  `Subscriber.dropped_messages`). The default stays unbounded.
- `Broadcast(..., on_reconnect=callback)`: called with the re-subscribed
  channels and the outage length after a Redis or Postgres backend
  reconnects, so applications can reload state that may have been missed.
  Custom backends can trigger it with `self._notify_reconnect(channels,
  downtime)`.
- Logging on the `broadcaster` logger for connection loss, reconnects and
  failed reconnects. The Redis and Postgres reader tasks are supervised: if
  one ever stops unexpectedly it is logged and restarted.
- Includes the unreleased changes from encode/broadcaster master after 0.3.1:
  public `broadcaster.backends` module (the old `broadcaster._backends` path
  still works), `RedisBackend(conn=...)` for a preconfigured client, and
  typing fixes.
