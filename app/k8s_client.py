"""Shared, lazily-initialised Kubernetes API clients and a small TTL cache.

Why this exists: the dashboard used to call ``config.load_incluster_config()`` and build
fresh ``CoreV1Api`` / ``AppsV1Api`` objects on *every* request. Each new ApiClient opens its
own connection pool, so every dashboard refresh re-negotiated TLS with the API server.
A single set of clients is thread-safe (urllib3 pools are) and much cheaper.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Callable

from kubernetes import client, config

_init_lock = threading.Lock()
_loaded = False
_apis: dict[str, Any] = {}


def ensure_config() -> None:
    """Load in-cluster config once (falls back to ~/.kube/config for local dev)."""
    global _loaded
    if _loaded:
        return
    with _init_lock:
        if _loaded:
            return
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        _loaded = True


def _api(name: str, factory: Callable[[], Any]) -> Any:
    ensure_config()
    api = _apis.get(name)
    if api is None:
        with _init_lock:
            api = _apis.get(name)
            if api is None:
                api = _apis[name] = factory()
    return api


def core_v1() -> client.CoreV1Api:
    return _api("core", client.CoreV1Api)


def apps_v1() -> client.AppsV1Api:
    return _api("apps", client.AppsV1Api)


def networking_v1() -> client.NetworkingV1Api:
    return _api("net", client.NetworkingV1Api)


def custom_objects() -> client.CustomObjectsApi:
    return _api("custom", client.CustomObjectsApi)


class TTLCache:
    """Tiny thread-safe cache with per-key locks, so N dashboards polling the same
    key trigger one Kubernetes query per TTL window instead of N."""

    def __init__(self) -> None:
        self._data: dict[Any, tuple[float, Any]] = {}
        self._locks: dict[Any, threading.Lock] = {}
        self._guard = threading.Lock()

    def get_or_set(self, key: Any, ttl: float, producer: Callable[[], Any]) -> Any:
        now = time.monotonic()
        hit = self._data.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
        with self._guard:
            lock = self._locks.setdefault(key, threading.Lock())
        with lock:
            hit = self._data.get(key)
            if hit and time.monotonic() - hit[0] < ttl:
                return hit[1]
            value = producer()  # exceptions propagate and are *not* cached
            self._data[key] = (time.monotonic(), value)
            # keep the cache bounded (namespace keys only, but be safe)
            if len(self._data) > 64:
                oldest = sorted(self._data.items(), key=lambda kv: kv[1][0])[:16]
                for k, _ in oldest:
                    self._data.pop(k, None)
            return value

    def clear(self) -> None:
        with self._guard:
            self._data.clear()
