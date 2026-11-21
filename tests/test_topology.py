import pytest

from app import topology
from app.config import settings
from app.topology import Snapshot, build_graph, build_summary
from tests import factories as f

NS = "observability"


def full_snapshot(**override):
    pg = {"app": "pg"}
    rca = {"app": "rca"}
    snap = Snapshot(
        namespaces=[f.namespace(NS)],
        deployments=[
            f.deployment("clusterpulse-postgres", NS, pg, volumes=[f.pvc_volume("pg-data")]),
            f.deployment("rca-agent", NS, rca, env_from_cm="rca-config", sa="rca-agent"),
        ],
        replicasets=[f.replicaset("pg-abc", NS, "clusterpulse-postgres"), f.replicaset("rca-abc", NS, "rca-agent")],
        pods=[f.pod("pg-abc-1", NS, pg, f.owner("ReplicaSet", "pg-abc")),
              f.pod("rca-abc-1", NS, rca, f.owner("ReplicaSet", "rca-abc"))],
        services=[f.service("clusterpulse-postgres", NS, pg, 5432), f.service("rca-agent", NS, rca, 8080)],
        pvcs=[f.pvc("pg-data", NS)],
        configmaps=[f.configmap("rca-config", NS, 18), f.configmap("kube-root-ca.crt", NS, 1)],
        serviceaccounts=[f.serviceaccount("rca-agent", NS), f.serviceaccount("default", NS)],
        ingressroutes=[f.ingressroute("rca-https", NS, "cp.local", "rca-agent", ["basic-auth"])],
        middlewares=[f.middleware("basic-auth", NS)],
    )
    for k, v in override.items():
        setattr(snap, k, v)
    return snap


def edge_set(graph):
    return {(e["source"], e["target"], e["type"]) for e in graph["edges"]}


def nid(kind, name, ns=NS):
    return f"{kind}/{ns}/{name}"


def test_full_chain_matches_expected_layout():
    g = build_graph(full_snapshot(), NS)
    edges = edge_set(g)
    # Middleware -> IngressRoute -> Service -> Deployment -> Pod
    assert (nid("Middleware", "basic-auth"), nid("IngressRoute", "rca-https"), "middleware") in edges
    assert (nid("IngressRoute", "rca-https"), nid("Service", "rca-agent"), "routes") in edges
    assert (nid("Service", "rca-agent"), nid("Deployment", "rca-agent"), "selects") in edges
    assert (nid("Deployment", "rca-agent"), nid("Pod", "rca-abc-1"), "owns") in edges
    # side inputs into the workload
    assert (nid("PVC", "pg-data"), nid("Deployment", "clusterpulse-postgres"), "mounts") in edges
    assert (nid("ConfigMap", "rca-config"), nid("Deployment", "rca-agent"), "config") in edges
    assert (nid("ServiceAccount", "rca-agent"), nid("Deployment", "rca-agent"), "identity") in edges
    assert (nid("Service", "clusterpulse-postgres"), nid("Deployment", "clusterpulse-postgres"), "selects") in edges


def test_noise_is_suppressed():
    g = build_graph(full_snapshot(), NS)
    ids = {n["id"] for n in g["nodes"]}
    assert nid("ConfigMap", "kube-root-ca.crt") not in ids       # injected into every namespace
    assert nid("ServiceAccount", "default") not in ids          # every workload would link to it
    assert not any(n["ghost"] for n in g["nodes"])


def test_ready_status_text_and_health():
    snap = full_snapshot()
    snap.deployments[1] = f.deployment("rca-agent", NS, {"app": "rca"}, replicas=3, ready=1)
    g = build_graph(snap, NS)
    dep = next(n for n in g["nodes"] if n["id"] == nid("Deployment", "rca-agent"))
    assert dep["status"] == "1/3 ready" and dep["health"] == "warn"
    snap.deployments[1] = f.deployment("rca-agent", NS, {"app": "rca"}, replicas=2, ready=0)
    dep = next(n for n in build_graph(snap, NS)["nodes"] if n["id"] == nid("Deployment", "rca-agent"))
    assert dep["health"] == "bad"


@pytest.mark.parametrize("kwargs,health,text", [
    (dict(), "ok", "Running"),
    (dict(waiting="CrashLoopBackOff", ready=False, restarts=9), "bad", "CrashLoopBackOff"),
    (dict(phase="Pending", ready=False), "warn", "Pending"),
    (dict(phase="Failed", ready=False), "bad", "Failed"),
    (dict(phase="Succeeded", ready=False), "neutral", "Completed"),
    (dict(ready=False), "warn", "Not ready 0/1"),
])
def test_pod_health(kwargs, health, text):
    snap = full_snapshot()
    snap.pods = [f.pod("pg-abc-1", NS, {"app": "pg"}, f.owner("ReplicaSet", "pg-abc"), **kwargs)]
    n = next(x for x in build_graph(snap, NS)["nodes"] if x["kind"] == "Pod")
    assert (n["health"], n["status"]) == (health, text)


def test_service_with_no_ready_backend_is_flagged():
    snap = full_snapshot()
    snap.pods = [f.pod("rca-abc-1", NS, {"app": "rca"}, f.owner("ReplicaSet", "rca-abc"), ready=False)]
    svc = next(n for n in build_graph(snap, NS)["nodes"] if n["id"] == nid("Service", "rca-agent"))
    assert svc["health"] == "bad" and svc["meta"]["endpointsReady"] == "0/1"
    svc2 = next(n for n in build_graph(snap, NS)["nodes"] if n["id"] == nid("Service", "clusterpulse-postgres"))
    assert svc2["health"] == "warn"  # selector matches no pod at all


def test_missing_references_become_ghost_nodes():
    snap = full_snapshot(configmaps=[], pvcs=[])
    snap.ingressroutes = [f.ingressroute("r", NS, "h", "nope", ["gone"])]
    snap.middlewares = []
    g = build_graph(snap, NS)
    ghosts = {n["id"] for n in g["nodes"] if n["ghost"]}
    assert {nid("ConfigMap", "rca-config"), nid("PVC", "pg-data"), nid("Service", "nope"),
            nid("Middleware", "gone")} <= ghosts
    assert all(n["health"] == "bad" and n["status"] == "Missing" for n in g["nodes"] if n["ghost"])


def test_no_false_ghosts_when_a_kind_could_not_be_listed():
    """403 on configmaps must degrade gracefully, never report every ConfigMap as missing."""
    snap = full_snapshot(configmaps=[])
    snap.skipped = {"configmaps", "serviceaccounts", "middlewares"}
    snap.serviceaccounts, snap.middlewares = [], []
    g = build_graph(snap, NS)
    assert not any(n["ghost"] for n in g["nodes"])


def test_cross_namespace_reference_is_not_a_false_ghost():
    snap = full_snapshot()
    snap.ingressroutes = [f.ingressroute("r", NS, "h", "rca-agent", ["mw@kubernetescrd"])]
    snap.ingressroutes[0]["spec"]["routes"][0]["middlewares"] = [{"name": "mw", "namespace": "other-ns"}]
    g = build_graph(snap, NS)  # `other-ns` is outside this snapshot
    assert not any(n["ghost"] for n in g["nodes"])


def test_non_kubernetes_traefik_providers_are_ignored():
    snap = full_snapshot()
    snap.ingressroutes = [f.ingressroute("r", NS, "h", "rca-agent", ["dashboard@internal", "x@file"])]
    g = build_graph(snap, NS)
    assert not any(n["kind"] == "Middleware" and n["ghost"] for n in g["nodes"])


def test_ingress_to_service():
    snap = full_snapshot(ingresses=[f.ingress("web", NS, "x.example", "rca-agent")])
    assert (nid("Ingress", "web"), nid("Service", "rca-agent"), "routes") in edge_set(build_graph(snap, NS))


def test_pod_cap_folds_extra_pods(monkeypatch):
    monkeypatch.setattr(settings, "topology_max_pods_per_workload", 3)
    labels = {"app": "rca"}
    snap = full_snapshot()
    snap.pods = [f.pod(f"p{i}", NS, labels, f.owner("ReplicaSet", "rca-abc"), phase="Running" if i else "Failed",
                       ready=bool(i)) for i in range(10)]
    g = build_graph(snap, NS)
    pods = [n for n in g["nodes"] if n["kind"] == "Pod"]
    groups = [n for n in g["nodes"] if n["kind"] == "PodGroup"]
    assert len(pods) == 3 and len(groups) == 1 and groups[0]["status"] == "+7 more pods"
    assert any(p["health"] == "bad" for p in pods), "unhealthy pods must be drawn first"
    # the summary still reports the true pod count
    summary = build_summary(snap, g)
    assert summary["totals"]["pods"] == 10


def test_node_cap_drops_low_value_nodes_first(monkeypatch):
    monkeypatch.setattr(settings, "topology_max_nodes", 8)
    g = build_graph(full_snapshot(), NS)
    assert g["truncated"] is True and len(g["nodes"]) == 8
    kinds = {n["kind"] for n in g["nodes"]}
    assert {"Service", "Deployment"} <= kinds
    ids = {n["id"] for n in g["nodes"]}
    assert all(e["source"] in ids and e["target"] in ids for e in g["edges"]), "no dangling edges"


def test_statefulset_volume_claim_templates():
    from kubernetes import client as k
    labels = {"app": "db"}
    sts = k.V1StatefulSet(
        metadata=f.meta("db", NS, labels),
        spec=k.V1StatefulSetSpec(
            replicas=1, service_name="db", selector=k.V1LabelSelector(match_labels=labels),
            template=k.V1PodTemplateSpec(metadata=k.V1ObjectMeta(labels=labels),
                                         spec=k.V1PodSpec(containers=[k.V1Container(name="c", image="i")])),
            volume_claim_templates=[k.V1PersistentVolumeClaim(metadata=k.V1ObjectMeta(name="data"))]),
        status=k.V1StatefulSetStatus(replicas=1, ready_replicas=1))
    snap = Snapshot(namespaces=[f.namespace(NS)], statefulsets=[sts], pvcs=[f.pvc("data-db-0", NS)],
                    pods=[f.pod("db-0", NS, labels, f.owner("StatefulSet", "db"))],
                    services=[f.service("db", NS, labels)])
    edges = edge_set(build_graph(snap, NS))
    assert (nid("PVC", "data-db-0"), nid("StatefulSet", "db"), "mounts") in edges
    assert (nid("StatefulSet", "db"), nid("Pod", "db-0"), "owns") in edges
    assert (nid("Service", "db"), nid("StatefulSet", "db"), "selects") in edges


def test_orphan_pod_links_service_directly():
    snap = Snapshot(namespaces=[f.namespace(NS)], pods=[f.pod("lonely", NS, {"app": "x"})],
                    services=[f.service("x", NS, {"app": "x"})])
    assert (nid("Service", "x"), nid("Pod", "lonely"), "selects") in edge_set(build_graph(snap, NS))


def test_replicaset_list_unavailable_falls_back_to_name_match():
    """Real pods are named <deployment>-<rs-hash>-<id>; used when ReplicaSets can't be listed."""
    snap = full_snapshot(replicasets=[])
    snap.pods = [f.pod("rca-agent-7d9c-xk2lp", NS, {"app": "rca"}, f.owner("ReplicaSet", "rca-agent-7d9c"))]
    assert (nid("Deployment", "rca-agent"), nid("Pod", "rca-agent-7d9c-xk2lp"), "owns") in edge_set(build_graph(snap, NS))


def test_revision_changes_only_when_graph_changes():
    a = build_graph(full_snapshot(), NS)
    b = build_graph(full_snapshot(), NS)
    assert a["revision"] == b["revision"]
    snap = full_snapshot()
    snap.pods[0] = f.pod("pg-abc-1", NS, {"app": "pg"}, f.owner("ReplicaSet", "pg-abc"), ready=False)
    assert build_graph(snap, NS)["revision"] != a["revision"]


def test_summary_shape_and_counts():
    snap = full_snapshot()
    summary = build_summary(snap, build_graph(snap, NS))
    ns = summary["namespaces"][0]
    assert (ns["pods"], ns["services"], ns["workloads"], ns["pvcs"], ns["ingresses"]) == (2, 2, 2, 1, 1)
    assert ns["config"] == 3  # rca-config, rca-agent SA, basic-auth middleware
    assert summary["totals"]["resources"] == ns["resources"]
    kinds = {r["kind"] for r in summary["resources"][NS]}
    assert {"Pod", "Service", "Deployment", "PVC", "IngressRoute", "ConfigMap"} <= kinds
    assert all({"namespace", "from", "to", "type"} <= set(e) for e in summary["edges"])


def test_excluded_namespace_never_appears(monkeypatch):
    monkeypatch.setattr(settings, "monitor_excluded_namespaces", "kube-system")
    topology._cache.clear()
    snap = full_snapshot()
    snap.pods.append(f.pod("coredns", "kube-system", {"k8s-app": "dns"}))
    # emulate fetch_snapshot's filter step
    snap.pods = [p for p in snap.pods if p.metadata.namespace not in settings.excluded_namespaces]
    assert "kube-system" not in {n["namespace"] for n in build_graph(snap, NS)["nodes"]}
