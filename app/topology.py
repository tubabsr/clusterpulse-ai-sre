"""Read-only resource topology: inventory snapshot -> typed graph (nodes + edges).

The graph follows the way Kubernetes objects actually relate, left to right:

    Middleware -> IngressRoute -> Service -> Deployment|StatefulSet|DaemonSet -> Pod
    Ingress ----------------------^              ^   ^   ^
    PVC / ConfigMap / ServiceAccount ------------+---+---+

Nothing here mutates the cluster. Secrets are never listed. ConfigMap contents are never
returned (only the number of keys).
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from kubernetes.client.rest import ApiException

from app import k8s_client
from app.config import settings

log = logging.getLogger("topology")

# kinds shown in the namespace summary cards
CARD_CATEGORIES = {
    "Pod": "pods",
    "Service": "services",
    "Deployment": "workloads",
    "StatefulSet": "workloads",
    "DaemonSet": "workloads",
    "Ingress": "ingresses",
    "IngressRoute": "ingresses",
    "PVC": "pvcs",
    "ConfigMap": "config",
    "ServiceAccount": "config",
    "Middleware": "config",
}
WORKLOAD_KINDS = ("Deployment", "StatefulSet", "DaemonSet")
BAD_WAITING = {"CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull", "CreateContainerConfigError",
               "CreateContainerError", "InvalidImageName", "RunContainerError"}
# nodes dropped first when a graph exceeds topology_max_nodes
DROP_PRIORITY = ["ConfigMap", "ServiceAccount", "PodGroup", "Pod"]

_cache = k8s_client.TTLCache()


# --------------------------------------------------------------------------- snapshot
@dataclass
class Snapshot:
    namespaces: list = field(default_factory=list)
    pods: list = field(default_factory=list)
    services: list = field(default_factory=list)
    deployments: list = field(default_factory=list)
    statefulsets: list = field(default_factory=list)
    daemonsets: list = field(default_factory=list)
    replicasets: list = field(default_factory=list)
    pvcs: list = field(default_factory=list)
    ingresses: list = field(default_factory=list)
    configmaps: list = field(default_factory=list)
    serviceaccounts: list = field(default_factory=list)
    ingressroutes: list = field(default_factory=list)  # dicts (custom resources)
    middlewares: list = field(default_factory=list)    # dicts (custom resources)
    warnings: list = field(default_factory=list)
    skipped: set = field(default_factory=set)  # kinds that could not be listed (RBAC / CRD)


class TopologyError(Exception):
    def __init__(self, status: int, detail: str):
        super().__init__(detail)
        self.status = status
        self.detail = detail


def _list_custom(plural: str, namespace: str | None) -> list:
    """Traefik CRDs. Tries the current API group, then the legacy one. Missing CRD -> []."""
    api = k8s_client.custom_objects()
    last: ApiException | None = None
    for group in ("traefik.io", "traefik.containo.us"):
        try:
            if namespace:
                data = api.list_namespaced_custom_object(group, "v1alpha1", namespace, plural)
            else:
                data = api.list_cluster_custom_object(group, "v1alpha1", plural)
            return data.get("items", [])
        except ApiException as exc:
            last = exc
            if exc.status == 404:  # CRD / group not installed
                continue
            raise
    if last and last.status != 404:
        raise last
    return []


def fetch_snapshot(namespace: str | None = None) -> Snapshot:
    """Query the API server concurrently. Core kinds are mandatory; everything else is
    best-effort so a missing RBAC rule or CRD degrades the graph instead of breaking it."""
    v1, apps, net = k8s_client.core_v1(), k8s_client.apps_v1(), k8s_client.networking_v1()
    ns = namespace or None

    def pick(namespaced, cluster_wide):
        return (lambda: namespaced(ns)) if ns else cluster_wide

    required: dict[str, Callable[[], Any]] = {
        "namespaces": v1.list_namespace,
        "pods": pick(lambda n: v1.list_namespaced_pod(n), v1.list_pod_for_all_namespaces),
        "services": pick(lambda n: v1.list_namespaced_service(n), v1.list_service_for_all_namespaces),
        "deployments": pick(lambda n: apps.list_namespaced_deployment(n), apps.list_deployment_for_all_namespaces),
        "pvcs": pick(lambda n: v1.list_namespaced_persistent_volume_claim(n),
                     v1.list_persistent_volume_claim_for_all_namespaces),
        "ingresses": pick(lambda n: net.list_namespaced_ingress(n), net.list_ingress_for_all_namespaces),
    }
    optional: dict[str, Callable[[], Any]] = {
        "statefulsets": pick(lambda n: apps.list_namespaced_stateful_set(n), apps.list_stateful_set_for_all_namespaces),
        "daemonsets": pick(lambda n: apps.list_namespaced_daemon_set(n), apps.list_daemon_set_for_all_namespaces),
        "replicasets": pick(lambda n: apps.list_namespaced_replica_set(n), apps.list_replica_set_for_all_namespaces),
        "serviceaccounts": pick(lambda n: v1.list_namespaced_service_account(n), v1.list_service_account_for_all_namespaces),
        "ingressroutes": lambda: _list_custom("ingressroutes", ns),
        "middlewares": lambda: _list_custom("middlewares", ns),
    }
    if settings.topology_include_configmaps:
        optional["configmaps"] = pick(lambda n: v1.list_namespaced_config_map(n), v1.list_config_map_for_all_namespaces)

    snap = Snapshot()
    if not settings.topology_include_configmaps:
        snap.skipped.add("configmaps")
    with ThreadPoolExecutor(max_workers=8, thread_name_prefix="topo") as pool:
        futures = {k: pool.submit(fn) for k, fn in {**required, **optional}.items()}
        for key, fut in futures.items():
            try:
                result = fut.result(timeout=30)
            except ApiException as exc:
                if key in required:
                    raise TopologyError(502, f"Topology inventory unavailable ({key}): {exc.reason}") from exc
                snap.skipped.add(key)
                snap.warnings.append(f"{key}: {exc.reason or exc.status} - skipped"
                                     + (" (grant list permission in RBAC to include it)" if exc.status == 403 else ""))
                continue
            except Exception as exc:  # network / timeout
                if key in required:
                    raise TopologyError(502, f"Topology inventory unavailable ({key}): {exc}") from exc
                snap.skipped.add(key)
                snap.warnings.append(f"{key}: {exc} - skipped")
                continue
            items = result if isinstance(result, list) else getattr(result, "items", result)
            setattr(snap, key, list(items or []))

    excluded = settings.excluded_namespaces

    def keep(obj) -> bool:
        meta = obj["metadata"] if isinstance(obj, dict) else obj.metadata
        name_space = meta.get("namespace") if isinstance(meta, dict) else meta.namespace
        return name_space not in excluded

    snap.namespaces = [n for n in snap.namespaces if n.metadata.name not in excluded
                       and (ns is None or n.metadata.name == ns)]
    for attr in ("pods", "services", "deployments", "statefulsets", "daemonsets", "replicasets", "pvcs",
                 "ingresses", "configmaps", "serviceaccounts", "ingressroutes", "middlewares"):
        setattr(snap, attr, [o for o in getattr(snap, attr) if keep(o)])
    return snap


# --------------------------------------------------------------------------- helpers
def _nid(kind: str, ns: str, name: str) -> str:
    return f"{kind}/{ns}/{name}"


def _subset(selector: dict | None, labels: dict | None) -> bool:
    return bool(selector) and all((labels or {}).get(k) == v for k, v in selector.items())


def _ports(svc) -> str:
    ports = svc.spec.ports or []
    return ", ".join(f":{p.port}" for p in ports[:4]) + (" …" if len(ports) > 4 else "")


def _pod_state(pod) -> tuple[str, str, dict]:
    """-> (status text, health, meta)"""
    status = pod.status
    phase = (status.phase if status else None) or "Unknown"
    cstat = (status.container_statuses if status else None) or []
    total = len(pod.spec.containers or []) if pod.spec else len(cstat)
    ready = sum(1 for c in cstat if c.ready)
    restarts = sum(c.restart_count or 0 for c in cstat)
    reason = ""
    for c in cstat:
        st = c.state
        if st and st.waiting and st.waiting.reason:
            reason = st.waiting.reason
            break
        if st and st.terminated and st.terminated.reason and st.terminated.reason != "Completed":
            reason = st.terminated.reason
            break
    if not reason and status and status.reason:
        reason = status.reason  # e.g. Evicted
    terminating = bool(pod.metadata.deletion_timestamp)

    if terminating:
        health, text = "warn", "Terminating"
    elif phase == "Succeeded":
        health, text = "neutral", "Completed"
    elif phase in ("Failed", "Unknown") or reason in BAD_WAITING or reason in ("Evicted", "OOMKilled"):
        health, text = "bad", reason or phase
    elif phase == "Pending":
        health, text = "warn", reason or "Pending"
    elif total and ready < total:
        health, text = "warn", reason or f"Not ready {ready}/{total}"
    else:
        health, text = "ok", "Running"
    meta = {"phase": phase, "ready": f"{ready}/{total}", "restarts": restarts,
            "node": (pod.spec.node_name if pod.spec else "") or "",
            "containers": [c.name for c in (pod.spec.containers or [])] if pod.spec else []}
    return text, health, meta


def _replica_health(ready: int, desired: int) -> tuple[str, str]:
    if desired == 0:
        return "Scaled to 0", "neutral"
    if ready >= desired:
        return f"{ready}/{desired} ready", "ok"
    return f"{ready}/{desired} ready", ("bad" if ready == 0 else "warn")


def _template_refs(pod_spec) -> dict[str, set]:
    """Names of PVCs / ConfigMaps referenced by a pod spec (volumes, envFrom, env valueFrom)."""
    refs: dict[str, set] = {"pvc": set(), "cm": set(), "cm_optional": set()}
    if not pod_spec:
        return refs
    for vol in pod_spec.volumes or []:
        if vol.persistent_volume_claim:
            refs["pvc"].add(vol.persistent_volume_claim.claim_name)
        if vol.config_map and vol.config_map.name:
            (refs["cm_optional"] if vol.config_map.optional else refs["cm"]).add(vol.config_map.name)
        if vol.projected:
            for src in vol.projected.sources or []:
                if src.config_map and src.config_map.name:
                    (refs["cm_optional"] if src.config_map.optional else refs["cm"]).add(src.config_map.name)
    for c in list(pod_spec.containers or []) + list(pod_spec.init_containers or []):
        for ef in c.env_from or []:
            if ef.config_map_ref and ef.config_map_ref.name:
                (refs["cm_optional"] if ef.config_map_ref.optional else refs["cm"]).add(ef.config_map_ref.name)
        for env in c.env or []:
            kr = env.value_from.config_map_key_ref if env.value_from else None
            if kr and kr.name:
                (refs["cm_optional"] if kr.optional else refs["cm"]).add(kr.name)
    return refs


_HOST_RE = re.compile(r"Host(?:SNI)?\(\s*((?:`[^`]*`\s*,?\s*)+)\)")


def _route_hosts(routes: list) -> list[str]:
    hosts: list[str] = []
    for route in routes:
        for group in _HOST_RE.findall(route.get("match", "") or ""):
            hosts += re.findall(r"`([^`]*)`", group)
    return sorted(set(hosts))


# --------------------------------------------------------------------------- graph
def build_graph(snap: Snapshot, namespace: str | None = None) -> dict:
    nodes: dict[str, dict] = {}
    edges: dict[tuple, dict] = {}

    def add_node(kind, ns, name, status="", health="neutral", detail="", meta=None, ghost=False):
        nid = _nid(kind, ns, name)
        nodes[nid] = {"id": nid, "kind": kind, "namespace": ns, "name": name, "status": status,
                      "health": health, "detail": detail, "meta": meta or {}, "ghost": ghost}
        return nid

    def add_edge(src, dst, etype):
        if src in nodes and dst in nodes and src != dst:
            edges[(src, dst, etype)] = {"source": src, "target": dst, "type": etype}

    known_ns = {n.metadata.name for n in snap.namespaces}
    kind_listed = {"PVC": True, "Service": True, "ConfigMap": "configmaps" not in snap.skipped,
                   "ServiceAccount": "serviceaccounts" not in snap.skipped,
                   "Middleware": "middlewares" not in snap.skipped}

    def ghost(kind, ns, name):
        """Placeholder for a referenced-but-missing object (typical FailedMount / dangling route cause).
        Only drawn when we really listed that kind in that namespace, to avoid false alarms."""
        if ns not in known_ns or not kind_listed.get(kind, False):
            return ""
        nid = _nid(kind, ns, name)
        if nid not in nodes:
            add_node(kind, ns, name, "Missing", "bad", "Referenced but not found in this namespace", ghost=True)
        return nid

    # ---- workloads -------------------------------------------------------------
    workloads: dict[str, Any] = {}  # nid -> k8s object
    for kind, items in (("Deployment", snap.deployments), ("StatefulSet", snap.statefulsets),
                        ("DaemonSet", snap.daemonsets)):
        for w in items:
            ns, name = w.metadata.namespace, w.metadata.name
            if kind == "DaemonSet":
                desired, ready = w.status.desired_number_scheduled or 0, w.status.number_ready or 0
            else:
                desired, ready = w.spec.replicas if w.spec.replicas is not None else 1, w.status.ready_replicas or 0
            text, health = _replica_health(ready, desired)
            images = [c.image for c in (w.spec.template.spec.containers or []) if c.image]
            nid = add_node(kind, ns, name, text, health, "Images: " + ", ".join(images) if images else "",
                           {"ready": ready, "desired": desired, "images": images})
            workloads[nid] = w

    # ---- pods + ownership ------------------------------------------------------
    rs_owner: dict[tuple, str] = {}  # (ns, rs name) -> workload nid
    for rs in snap.replicasets:
        for ref in rs.metadata.owner_references or []:
            if ref.controller and ref.kind == "Deployment":
                rs_owner[(rs.metadata.namespace, rs.metadata.name)] = _nid("Deployment", rs.metadata.namespace, ref.name)

    def owner_of(pod) -> str | None:
        ns = pod.metadata.namespace
        for ref in pod.metadata.owner_references or []:
            if not ref.controller:
                continue
            if ref.kind == "ReplicaSet":
                nid = rs_owner.get((ns, ref.name))
                if nid in nodes:
                    return nid
                # ReplicaSet list unavailable: fall back to selector + name prefix
                for wid, w in workloads.items():
                    if wid.startswith(f"Deployment/{ns}/") and pod.metadata.name.startswith(w.metadata.name + "-") \
                            and _subset((w.spec.selector.match_labels if w.spec.selector else None), pod.metadata.labels):
                        return wid
            elif ref.kind in ("StatefulSet", "DaemonSet"):
                nid = _nid(ref.kind, ns, ref.name)
                if nid in nodes:
                    return nid
        return None

    pods_by_owner: dict[str, list] = {}
    orphan_pods: list = []
    pod_nid: dict[tuple, str] = {}
    pod_state: dict[str, tuple] = {}
    for pod in snap.pods:
        text, health, meta = _pod_state(pod)
        meta["owner"] = None
        pod_state[pod.metadata.name + "|" + pod.metadata.namespace] = (text, health, meta)
        owner = owner_of(pod)
        (pods_by_owner.setdefault(owner, []) if owner else orphan_pods).append(pod)

    sev = {"bad": 0, "warn": 1, "neutral": 2, "ok": 3}
    cap = max(1, settings.topology_max_pods_per_workload)

    def emit_pods(pods, owner_nid):
        pods = sorted(pods, key=lambda p: (sev[pod_state[p.metadata.name + "|" + p.metadata.namespace][1]], p.metadata.name))
        shown, hidden = pods[:cap], pods[cap:]
        for pod in shown:
            ns, name = pod.metadata.namespace, pod.metadata.name
            text, health, meta = pod_state[name + "|" + ns]
            meta["owner"] = owner_nid
            nid = add_node("Pod", ns, name, text, health, f"Node: {meta['node'] or 'Unassigned'}", meta)
            pod_nid[(ns, name)] = nid
            if owner_nid:
                add_edge(owner_nid, nid, "owns")
        if hidden:
            ns = hidden[0].metadata.namespace
            worst = min((pod_state[p.metadata.name + "|" + p.metadata.namespace][1] for p in hidden), key=lambda h: sev[h])
            parts = owner_nid.split("/") if owner_nid else ["orphan", ns, "pods"]
            gid = add_node("PodGroup", ns, f"{parts[0].lower()}-{parts[2]}-more", f"+{len(hidden)} more pods", worst,
                           f"{len(hidden)} additional pods not drawn", {"hidden": len(hidden)})
            if owner_nid:
                add_edge(owner_nid, gid, "owns")
            for p in hidden:
                pod_nid[(p.metadata.namespace, p.metadata.name)] = gid

    for owner_nid, pods in pods_by_owner.items():
        emit_pods(pods, owner_nid)
    # orphan pods grouped per namespace so the cap applies per namespace
    by_ns: dict[str, list] = {}
    for pod in orphan_pods:
        by_ns.setdefault(pod.metadata.namespace, []).append(pod)
    for pods in by_ns.values():
        emit_pods(pods, None)

    # ---- services --------------------------------------------------------------
    service_ids: set[str] = set()
    wl_by_ns: dict[str, list] = {}
    for wid, w in workloads.items():
        wl_by_ns.setdefault(w.metadata.namespace, []).append((wid, w))
    pods_by_ns: dict[str, list] = {}
    for p in snap.pods:
        pods_by_ns.setdefault(p.metadata.namespace, []).append(p)
    for svc in snap.services:
        ns, name = svc.metadata.namespace, svc.metadata.name
        sel = svc.spec.selector or {}
        stype = svc.spec.type or "ClusterIP"
        matched_workloads = [wid for wid, w in wl_by_ns.get(ns, [])
                             if _subset(sel, w.spec.template.metadata.labels if w.spec.template.metadata else None)]
        matched_pods = [p for p in pods_by_ns.get(ns, []) if _subset(sel, p.metadata.labels)] if sel else []
        ready_pods = sum(1 for p in matched_pods if pod_state[p.metadata.name + "|" + ns][1] == "ok")
        if stype == "ExternalName" or not sel:
            health = "neutral"
        elif not matched_pods:
            health = "warn"
        elif ready_pods == 0:
            health = "bad"
        else:
            health = "ok" if ready_pods == len(matched_pods) else "warn"
        detail = f"{stype} {_ports(svc)}".strip()
        meta = {"type": stype, "clusterIP": svc.spec.cluster_ip, "selector": sel,
                "endpointsReady": f"{ready_pods}/{len(matched_pods)}" if sel else None}
        nid = add_node("Service", ns, name, f"{stype} {_ports(svc)}".strip(), health, detail, meta)
        service_ids.add(nid)
        for wid in matched_workloads:
            add_edge(nid, wid, "selects")
        if not matched_workloads:  # orphan pods / Jobs: link straight to pods
            for p in matched_pods[:cap]:
                add_edge(nid, pod_nid.get((ns, p.metadata.name), ""), "selects")

    # ---- PVC / ConfigMap / ServiceAccount -------------------------------------
    pvc_names = {(p.metadata.namespace, p.metadata.name) for p in snap.pvcs}
    for pvc in snap.pvcs:
        ns, name = pvc.metadata.namespace, pvc.metadata.name
        phase = (pvc.status.phase if pvc.status else None) or "Unknown"
        cap_s = ((pvc.status.capacity or {}).get("storage") if pvc.status else "") or \
                ((pvc.spec.resources.requests or {}).get("storage") if pvc.spec and pvc.spec.resources else "") or ""
        health = {"Bound": "ok", "Pending": "warn", "Lost": "bad"}.get(phase, "neutral")
        add_node("PVC", ns, name, f"{cap_s} ({phase})".strip(), health,
                 f"Storage: {cap_s or 'n/a'} · class: {pvc.spec.storage_class_name or 'default'}",
                 {"phase": phase, "storageClass": pvc.spec.storage_class_name, "volume": pvc.spec.volume_name})

    cm_names: set[tuple] = set()
    for cm in snap.configmaps:
        ns, name = cm.metadata.namespace, cm.metadata.name
        if name == "kube-root-ca.crt":
            continue  # injected into every namespace: pure noise
        keys = len(cm.data or {}) + len(cm.binary_data or {})
        cm_names.add((ns, name))
        add_node("ConfigMap", ns, name, f"{keys} keys", "neutral", f"{keys} keys", {"keys": keys})

    sa_names: set[tuple] = set()
    for sa in snap.serviceaccounts:
        sa_names.add((sa.metadata.namespace, sa.metadata.name))
        if sa.metadata.name != "default":
            add_node("ServiceAccount", sa.metadata.namespace, sa.metadata.name, "Workload identity", "neutral",
                     "Workload identity")

    pod_refs: dict[str, dict] = {}
    for wid, w in workloads.items():
        ns = w.metadata.namespace
        spec = w.spec.template.spec
        refs = _template_refs(spec)
        for claim in refs["pvc"]:
            add_edge(_nid("PVC", ns, claim) if (ns, claim) in pvc_names else ghost("PVC", ns, claim), wid, "mounts")
        if wid.startswith("StatefulSet/"):  # volumeClaimTemplates -> <tpl>-<sts>-<n>
            for tpl in (w.spec.volume_claim_templates or []):
                prefix = f"{tpl.metadata.name}-{w.metadata.name}-"
                for (pns, pname) in pvc_names:
                    if pns == ns and pname.startswith(prefix):
                        add_edge(_nid("PVC", ns, pname), wid, "mounts")
        if "configmaps" not in snap.skipped:
            for cm in refs["cm"]:
                nid = _nid("ConfigMap", ns, cm) if (ns, cm) in cm_names else (
                    ghost("ConfigMap", ns, cm) if cm != "kube-root-ca.crt" else "")
                add_edge(nid, wid, "config")
            for cm in refs["cm_optional"]:
                if (ns, cm) in cm_names:
                    add_edge(_nid("ConfigMap", ns, cm), wid, "config")
        sa = spec.service_account_name or spec.service_account
        if sa and sa != "default":
            add_edge(_nid("ServiceAccount", ns, sa) if (ns, sa) in sa_names else ghost("ServiceAccount", ns, sa),
                     wid, "identity")

    # orphan pods mount PVCs / use SAs directly
    for pod in orphan_pods:
        ns = pod.metadata.namespace
        nid = pod_nid.get((ns, pod.metadata.name))
        if not nid or nid not in nodes:
            continue
        refs = _template_refs(pod.spec)
        for claim in refs["pvc"]:
            if (ns, claim) in pvc_names:
                add_edge(_nid("PVC", ns, claim), nid, "mounts")
        for cm in refs["cm"] | refs["cm_optional"]:
            if (ns, cm) in cm_names:
                add_edge(_nid("ConfigMap", ns, cm), nid, "config")

    # ---- ingress layer ---------------------------------------------------------
    for ing in snap.ingresses:
        ns, name = ing.metadata.namespace, ing.metadata.name
        spec = ing.spec
        hosts = sorted({r.host for r in (spec.rules or []) if r.host}) if spec else []
        nid = add_node("Ingress", ns, name, hosts[0] if hosts else "No host rules", "neutral",
                       "Hosts: " + (", ".join(hosts) if hosts else "No host rules"), {"hosts": hosts})
        backends = set()
        if spec and spec.default_backend and spec.default_backend.service:
            backends.add(spec.default_backend.service.name)
        for rule in (spec.rules or []) if spec else []:
            for path in (rule.http.paths or []) if rule.http else []:
                if path.backend and path.backend.service:
                    backends.add(path.backend.service.name)
        for b in backends:
            tid = _nid("Service", ns, b) if _nid("Service", ns, b) in service_ids else ghost("Service", ns, b)
            add_edge(nid, tid, "routes")

    mw_index = {(m["metadata"]["namespace"], m["metadata"]["name"]): m for m in snap.middlewares}
    for mw in snap.middlewares:
        meta = mw["metadata"]
        spec = mw.get("spec") or {}
        mtype = next(iter(spec), "middleware")
        add_node("Middleware", meta["namespace"], meta["name"], mtype, "neutral", f"Type: {mtype}", {"type": mtype})

    for ir in snap.ingressroutes:
        meta = ir["metadata"]
        ns, name = meta["namespace"], meta["name"]
        spec = ir.get("spec") or {}
        routes = spec.get("routes") or []
        hosts = _route_hosts(routes)
        entry = ",".join(spec.get("entryPoints") or [])
        nid = add_node("IngressRoute", ns, name, hosts[0] if hosts else (entry or "IngressRoute"), "neutral",
                       "Hosts: " + (", ".join(hosts) if hosts else "n/a") + (f" · entryPoints: {entry}" if entry else ""),
                       {"hosts": hosts, "entryPoints": spec.get("entryPoints") or [], "tls": bool(spec.get("tls"))})
        for route in routes:
            for svc in route.get("services") or []:
                if svc.get("kind", "Service") != "Service" or not svc.get("name"):
                    continue
                sns = svc.get("namespace") or ns
                tid = _nid("Service", sns, svc["name"])
                add_edge(nid, tid if tid in service_ids else ghost("Service", sns, svc["name"]), "routes")
            for mref in route.get("middlewares") or []:
                mname, mns = mref.get("name", ""), mref.get("namespace") or ns
                if "@" in mname:
                    mname, provider = mname.split("@", 1)
                    if provider != "kubernetescrd":
                        continue  # file/internal providers are not Kubernetes objects
                if (mns, mname) in mw_index:
                    add_edge(_nid("Middleware", mns, mname), nid, "middleware")
                elif mname:
                    add_edge(ghost("Middleware", mns, mname), nid, "middleware")

    # ---- drop dangling edges, enforce node cap ---------------------------------
    truncated = False
    if len(nodes) > settings.topology_max_nodes:
        truncated = True
        for kind in DROP_PRIORITY:
            if len(nodes) <= settings.topology_max_nodes:
                break
            for nid in [n for n, v in nodes.items() if v["kind"] == kind]:
                if len(nodes) <= settings.topology_max_nodes:
                    break
                del nodes[nid]
    edge_list = sorted((e for e in edges.values() if e["source"] in nodes and e["target"] in nodes),
                       key=lambda e: (e["source"], e["target"], e["type"]))
    node_list = sorted(nodes.values(), key=lambda n: (n["namespace"], n["kind"], n["name"]))

    counts: dict[str, int] = {}
    for n in node_list:
        counts[n["kind"]] = counts.get(n["kind"], 0) + 1
    revision = hashlib.sha1(json.dumps([node_list, edge_list], sort_keys=True, default=str).encode()).hexdigest()[:16]
    return {"nodes": node_list, "edges": edge_list, "counts": counts, "truncated": truncated,
            "warnings": snap.warnings, "revision": revision, "namespace": namespace or None,
            "generated_at": datetime.now(timezone.utc).isoformat()}


def get_bundle(namespace: str | None = None) -> tuple[Snapshot, dict]:
    """(snapshot, graph) cached together: summary + graph endpoints share one K8s query round."""
    def produce():
        snap = fetch_snapshot(namespace)
        return snap, build_graph(snap, namespace)
    return _cache.get_or_set(("bundle", namespace or ""), settings.topology_cache_seconds, produce)


def get_graph(namespace: str | None = None) -> dict:
    return get_bundle(namespace)[1]


# --------------------------------------------------------------------------- summary
def build_summary(snap: Snapshot, graph: dict) -> dict:
    """Namespace cards + resource inventory + relationship table (backwards compatible shape)."""
    cats = ("pods", "services", "workloads", "ingresses", "pvcs", "config")
    groups = {n.metadata.name: {"namespace": n.metadata.name, **{c: 0 for c in cats}, "resources": 0}
              for n in snap.namespaces}
    resources: dict[str, list] = {name: [] for name in groups}
    for pod in snap.pods:  # full pod inventory, independent of the per-workload drawing cap
        if pod.metadata.namespace in groups:
            text, health, meta = _pod_state(pod)
            groups[pod.metadata.namespace]["pods"] += 1
            groups[pod.metadata.namespace]["resources"] += 1
            resources[pod.metadata.namespace].append({
                "kind": "Pod", "name": pod.metadata.name, "status": text, "health": health,
                "details": f"Node: {meta['node'] or 'Unassigned'} · ready {meta['ready']} · restarts {meta['restarts']}"})
    for n in graph["nodes"]:
        if n["ghost"] or n["namespace"] not in groups or n["kind"] in ("PodGroup", "Pod"):
            continue
        g = groups[n["namespace"]]
        g[CARD_CATEGORIES.get(n["kind"], "config")] += 1
        g["resources"] += 1
        resources[n["namespace"]].append({"kind": n["kind"], "name": n["name"], "status": n["status"],
                                          "health": n["health"], "details": n["detail"]})
    label = {n["id"]: f"{n['kind']}/{n['name']}" for n in graph["nodes"]}
    rels = [{"namespace": e["source"].split("/", 2)[1], "from": label[e["source"]],
             "to": label[e["target"]], "type": e["type"]} for e in graph["edges"]]
    totals = {c: sum(g[c] for g in groups.values()) for c in cats}
    totals["resources"] = sum(g["resources"] for g in groups.values())
    body = {"namespaces": list(groups.values()), "resources": resources, "edges": rels, "totals": totals,
            "warnings": graph["warnings"]}
    body["revision"] = hashlib.sha1(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()[:16]
    return body


def get_summary(namespace: str | None = None) -> dict:
    snap, graph = get_bundle(namespace)
    return build_summary(snap, graph)
