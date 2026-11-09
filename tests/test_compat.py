from __future__ import annotations

import importlib

import pytest


@pytest.mark.parametrize("module", ["base", "memory", "redis", "postgres", "kafka"])
def test_old_private_backend_paths_still_work(module: str) -> None:
    # encode/broadcaster 0.3.1 had the backends in `broadcaster._backends`;
    # master renamed it to `broadcaster.backends`. Both paths must work.
    old = importlib.import_module(f"broadcaster._backends.{module}")
    new = importlib.import_module(f"broadcaster.backends.{module}")
    public = [name for name in vars(new) if name.endswith("Backend")]
    assert public
    for name in public:
        assert getattr(old, name) is getattr(new, name)
