import pytest
from fastapi.testclient import TestClient

from app import main, topology
from app.config import settings
from tests import factories as f
from tests.test_topology import NS, full_snapshot


@pytest.fixture()
def client(monkeypatch):
    topology._cache.clear()
    monkeypatch.setattr(topology, "fetch_snapshot", lambda ns=None: full_snapshot())
    return TestClient(main.app)  # no lifespan: no DB, no watchers


def test_graph_endpoint_shape(client):
    r = client.get("/api/topology/graph", params={"namespace": NS})
    assert r.status_code == 200
    body = r.json()
    assert {"nodes", "edges", "counts", "revision", "truncated", "warnings", "generated_at"} <= set(body)
    assert body["namespace"] == NS
    node = body["nodes"][0]
    assert {"id", "kind", "name", "namespace", "status", "health", "detail", "meta", "ghost"} <= set(node)
    assert r.headers["cache-control"] == "no-store"


def test_summary_endpoint_shape(client):
    body = client.get("/api/topology").json()
    assert body["totals"]["pods"] == 2 and body["namespaces"][0]["namespace"] == NS
    assert "revision" in body


@pytest.mark.parametrize("bad", ["UPPER", "a_b", "-x", "x" * 64, "a b", "../etc"])
def test_namespace_is_validated(client, bad):
    assert client.get("/api/topology/graph", params={"namespace": bad}).status_code == 400


def test_excluded_namespace_is_hidden(client):
    assert client.get("/api/topology/graph", params={"namespace": "kube-system"}).status_code == 404
    assert client.get("/api/pods", params={"namespace": "kube-system"}).status_code == 404


def test_logs_endpoint_respects_exclusions(client):
    r = client.get("/api/logs", params={"namespace": "kube-system", "pod": "coredns"})
    assert r.status_code == 403


def test_unknown_namespace_is_404(monkeypatch):
    topology._cache.clear()
    monkeypatch.setattr(topology, "fetch_snapshot", lambda ns=None: topology.Snapshot())
    assert TestClient(main.app).get("/api/topology/graph", params={"namespace": "ghost"}).status_code == 404


def test_k8s_failure_maps_to_502(monkeypatch):
    topology._cache.clear()

    def boom(ns=None):
        raise topology.TopologyError(502, "Topology inventory unavailable (pods): Forbidden")
    monkeypatch.setattr(topology, "fetch_snapshot", boom)
    r = TestClient(main.app).get("/api/topology/graph", params={"namespace": NS})
    assert r.status_code == 502 and "Forbidden" in r.json()["detail"]


def test_failures_are_not_cached(monkeypatch):
    topology._cache.clear()
    calls = {"n": 0}

    def flaky(ns=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise topology.TopologyError(502, "down")
        return full_snapshot()
    monkeypatch.setattr(topology, "fetch_snapshot", flaky)
    c = TestClient(main.app)
    assert c.get("/api/topology/graph", params={"namespace": NS}).status_code == 502
    assert c.get("/api/topology/graph", params={"namespace": NS}).status_code == 200


def test_snapshot_cache_collapses_concurrent_polls(monkeypatch):
    topology._cache.clear()
    calls = {"n": 0}

    def counted(ns=None):
        calls["n"] += 1
        return full_snapshot()
    monkeypatch.setattr(topology, "fetch_snapshot", counted)
    c = TestClient(main.app)
    for _ in range(5):
        c.get("/api/topology/graph", params={"namespace": NS})
        c.get("/api/topology", params={"namespace": NS})
    assert calls["n"] == 1, "summary + graph + repeated polls must share one K8s query round"


def test_dashboard_and_security_headers(client):
    r = client.get("/")
    assert r.status_code == 200 and "ClusterPulse" in r.text
    assert r.headers["x-frame-options"] == "DENY"
    assert r.headers["x-content-type-options"] == "nosniff"
    assert "frame-ancestors 'none'" in r.headers["content-security-policy"]


def test_healthz_never_touches_db(client, monkeypatch):
    monkeypatch.setattr(main.engine, "connect", lambda: (_ for _ in ()).throw(RuntimeError("db down")))
    assert client.get("/healthz").status_code == 200


def test_readyz_reports_db_outage(client, monkeypatch):
    monkeypatch.setattr(main.engine, "connect", lambda: (_ for _ in ()).throw(RuntimeError("db down")))
    r = client.get("/readyz")
    assert r.status_code == 503 and r.json()["status"] == "not-ready"
