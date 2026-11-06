from __future__ import annotations

import asyncio
import time


async def stop_task(task: asyncio.Task[None], timeout: float) -> bool:
    """Cancel `task` and wait for it to finish, for at most `timeout` seconds.

    Cancels repeatedly: a cancellation swallowed by a client library (redis-py 8
    on Python 3.11 turns one that arrives inside a blocking read into an empty
    result) is then retried, and the next one lands where it is honoured.
    Unlike `await task`, asyncio.wait() does not raise the CancelledError we
    cause, while a cancellation of the caller itself still propagates.

    Returns True if the task finished.
    """
    deadline = time.monotonic() + timeout
    while True:
        task.cancel()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return task.done()
        done, _ = await asyncio.wait({task}, timeout=min(0.05, remaining))
        if done:
            return True


async def wait_or_stop(stop: asyncio.Event, delay: float) -> None:
    """Sleep for `delay` seconds, returning early if `stop` is set."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=delay)
    except asyncio.TimeoutError:
        pass


async def wait_any(events: list[asyncio.Event], timeout: float) -> bool:
    """Wait until any of `events` is set, for at most `timeout` seconds.

    Returns True if one was set.
    """
    waiters = [asyncio.ensure_future(event.wait()) for event in events]
    try:
        done, _ = await asyncio.wait(waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        return bool(done)
    finally:
        for waiter in waiters:
            waiter.cancel()
