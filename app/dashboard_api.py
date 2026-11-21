import re

from fastapi import APIRouter, HTTPException, Query
from kubernetes.client.rest import ApiException
import httpx
from app import k8s_client, topology
from app.config import settings

router = APIRouter(prefix="/api", tags=["dashboard"])

_DNS_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")


def core():
    # shared, thread-safe clients (config is loaded exactly once)
    return k8s_client.core_v1(), k8s_client.apps_v1()


def _check_namespace(namespace: str | None) -> str | None:
    """Validate user-supplied namespace and enforce the monitoring exclusion policy."""
    if not namespace:
        return None
    if len(namespace) > 63 or not _DNS_LABEL.match(namespace):
        raise HTTPException(status_code=400, detail="Invalid namespace name")
    if namespace in settings.excluded_namespaces:
        raise HTTPException(status_code=404, detail="Namespace is not monitored")
    return namespace

@router.get("/overview")
def overview():
    v1, apps = core()
    try:
        pods = v1.list_pod_for_all_namespaces().items
        nodes = v1.list_node().items
        deps = apps.list_deployment_for_all_namespaces().items
    except ApiException as e:
        raise HTTPException(status_code=502, detail=f"Kubernetes API error: {e.reason}")
    pods = [p for p in pods if p.metadata.namespace not in settings.excluded_namespaces]
    running = sum(p.status.phase == "Running" for p in pods)
    pending = sum(p.status.phase == "Pending" for p in pods)
    failed = sum(p.status.phase == "Failed" for p in pods)
    unknown = sum(p.status.phase == "Unknown" for p in pods)
    ready_pods = sum(bool(p.status.container_statuses) and all(c.ready for c in p.status.container_statuses) for p in pods)
    ready_nodes = sum(any(c.type == "Ready" and c.status == "True" for c in (n.status.conditions or [])) for n in nodes)
    desired = sum(d.spec.replicas or 0 for d in deps)
    available = sum(d.status.available_replicas or 0 for d in deps)
    health = (100 * ready_pods / len(pods)) if pods else 100.0
    return {"total_pods":len(pods),"running":running,"pending":pending,"failed":failed,
        "unknown":unknown,"unhealthy_pods":len(pods)-ready_pods,"ready_pods":ready_pods,
        "total_nodes":len(nodes),"ready_nodes":ready_nodes,"health_pct":round(health,1),
        "desired_replicas":desired,"available_replicas":available,
        "deployment_pct":round(100*available/desired,1) if desired else None}

@router.get("/nodes")
def nodes():
    v1, _ = core()
    try: items = v1.list_node().items
    except ApiException as e: raise HTTPException(status_code=502, detail=str(e))
    out=[]
    for n in items:
        cond={c.type:c.status=="True" for c in (n.status.conditions or [])}
        out.append({"name":n.metadata.name,"ready":cond.get("Ready",False),
            "disk_pressure":cond.get("DiskPressure",False),"pid_pressure":cond.get("PIDPressure",False),
            "memory_pressure":cond.get("MemoryPressure",False),
            "cpu":n.status.capacity.get("cpu") if n.status.capacity else None,
            "memory":n.status.capacity.get("memory") if n.status.capacity else None})
    return out

@router.get("/workloads")
def workloads(namespace: str | None = None):
    _, apps = core()
    out=[]
    try:
        if namespace:
            deps=apps.list_namespaced_deployment(namespace).items
            sts=apps.list_namespaced_stateful_set(namespace).items
            ds=apps.list_namespaced_daemon_set(namespace).items
            rs=apps.list_namespaced_replica_set(namespace).items
        else:
            deps=apps.list_deployment_for_all_namespaces().items
            sts=apps.list_stateful_set_for_all_namespaces().items
            ds=apps.list_daemon_set_for_all_namespaces().items
            rs=apps.list_replica_set_for_all_namespaces().items
    except ApiException as e: raise HTTPException(status_code=502, detail=str(e))
    for kind, items in [("Deployment",deps),("StatefulSet",sts),("DaemonSet",ds),("ReplicaSet",rs)]:
        for x in items:
            ns=x.metadata.namespace
            if ns in settings.excluded_namespaces: continue
            if kind == "DaemonSet":
                desired = x.status.desired_number_scheduled or 0
                ready = x.status.number_ready or 0
            else:
                desired = x.spec.replicas or 0
                ready = x.status.ready_replicas or 0
            out.append({"namespace":ns,"name":x.metadata.name,"kind":kind,"desired":desired,"ready":ready})
    return out


@router.get("/pods")
def pods(namespace: str | None = None, search: str | None = None, phase: str | None = None, limit: int = 1000):
    namespace = _check_namespace(namespace)
    v1, _ = core()
    try:
        # no server-side `limit`: it would truncate *before* excluded namespaces are filtered out
        items = (v1.list_namespaced_pod(namespace).items if namespace
                 else v1.list_pod_for_all_namespaces().items)
    except ApiException as e:
        raise HTTPException(status_code=502, detail=f"Kubernetes API error: {e.reason}")
    out = []
    needle = (search or "").lower()
    for pod in items:
        ns = pod.metadata.namespace or ""
        if ns in settings.excluded_namespaces:
            continue
        name = pod.metadata.name or ""
        statuses = pod.status.container_statuses or []
        restarts = sum(c.restart_count or 0 for c in statuses)
        ready = sum(1 for c in statuses if c.ready)
        total = len(statuses)
        state = pod.status.phase or "Unknown"
        waiting = [getattr(getattr(c, "state", None), "waiting", None) for c in statuses]
        reasons = [w.reason for w in waiting if w and w.reason]
        reason = reasons[0] if reasons else ""
        if phase and state.lower() != phase.lower():
            continue
        if needle and needle not in f"{ns}/{name} {pod.spec.node_name or ''}".lower():
            continue
        out.append({"namespace": ns, "name": name, "phase": state, "ready": ready,
                    "containers": total, "restarts": restarts, "reason": reason,
                    "node": pod.spec.node_name or "",
                    "created": pod.metadata.creation_timestamp.isoformat() if pod.metadata.creation_timestamp else None,
                    "labels": pod.metadata.labels or {},
                    "container_names": [c.name for c in (pod.spec.containers or [])]})
    # problems first (not Running, then most restarts), then stable by namespace/name
    out.sort(key=lambda x: (x["phase"] == "Running" and x["ready"] == x["containers"], -x["restarts"],
                            x["namespace"], x["name"]))
    return out[:min(max(limit, 1), 2000)]


@router.get("/logs")
def pod_logs(namespace: str, pod: str, container: str | None = None,
             tail: int = 200, previous: bool = False, timestamps: bool = True):
    if not namespace or not pod:
        raise HTTPException(status_code=400, detail="namespace and pod are required")
    if namespace in settings.excluded_namespaces:
        raise HTTPException(status_code=403, detail="Logs for this namespace are excluded from monitoring")
    v1, _ = core()
    try:
        result = v1.read_namespaced_pod_log(
            name=pod, namespace=namespace, container=container,
            tail_lines=min(max(tail, 1), 2000), previous=previous,
            timestamps=timestamps)
        return {"namespace": namespace, "pod": pod, "container": container,
                "previous": previous, "lines": result or ""}
    except ApiException as e:
        raise HTTPException(status_code=e.status or 502, detail=f"Unable to read pod logs: {e.reason}")

@router.get("/metrics")
async def metrics():
    if not settings.prometheus_url:
        return {"cpu_pct":None,"memory_pct":None,"configured":False}
    async def query(expr):
        async with httpx.AsyncClient(timeout=8) as c:
            r=await c.get(settings.prometheus_url.rstrip("/")+"/api/v1/query",params={"query":expr})
            r.raise_for_status()
            data=r.json()
            result=data.get("data",{}).get("result",[])
            return float(result[0]["value"][1]) if result else None
    try:
        cpu=await query('100 * (1 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m])))')
        mem=await query('100 * (1 - sum(node_memory_MemAvailable_bytes) / sum(node_memory_MemTotal_bytes))')
        return {"cpu_pct":round(cpu,1) if cpu is not None else None,
                "memory_pct":round(mem,1) if mem is not None else None,"configured":True}
    except Exception:
        return {"cpu_pct":None,"memory_pct":None,"configured":True,"error":"Prometheus query failed"}

# Read-only Kubernetes management views. These endpoints never mutate cluster state.
def _all_namespaces(v1):
    return [n.metadata.name for n in v1.list_namespace().items
            if n.metadata.name not in settings.excluded_namespaces]

@router.get("/namespaces")
def namespaces():
    v1, _ = core()
    try:
        return [{"name": n.metadata.name, "phase": getattr(n.status, "phase", "Unknown")}
                for n in v1.list_namespace().items
                if n.metadata.name not in settings.excluded_namespaces]
    except ApiException as e:
        raise HTTPException(status_code=502, detail=f"Kubernetes API error: {e.reason}")

def _topology_call(fn, *args):
    try:
        return fn(*args)
    except topology.TopologyError as exc:
        raise HTTPException(status_code=exc.status, detail=exc.detail)
    except ApiException as exc:
        raise HTTPException(status_code=502, detail=f"Topology inventory unavailable: {exc.reason}")


@router.get("/topology")
def topology_summary(namespace: str | None = Query(None)):
    """Namespace cards, resource inventory and relationship table (read-only)."""
    return _topology_call(topology.get_summary, _check_namespace(namespace))


@router.get("/topology/graph")
def topology_graph(namespace: str | None = Query(None)):
    """Typed resource graph: nodes (Service, Deployment, Pod, PVC, ConfigMap, ServiceAccount,
    Ingress, IngressRoute, Middleware ...) and directed edges. `revision` changes only when the
    graph changes, so clients can poll cheaply and skip re-rendering."""
    ns = _check_namespace(namespace)
    snap, graph = _topology_call(topology.get_bundle, ns)
    if ns and not snap.namespaces:
        raise HTTPException(status_code=404, detail="Namespace not found")
    return graph


@router.get("/applications")
def applications(namespace: str | None = None):
    namespace = _check_namespace(namespace)
    v1, apps = core()
    try:
        ds = apps.list_namespaced_deployment(namespace).items if namespace else apps.list_deployment_for_all_namespaces().items
        sts = apps.list_namespaced_stateful_set(namespace).items if namespace else apps.list_stateful_set_for_all_namespaces().items
        pods = v1.list_namespaced_pod(namespace).items if namespace else v1.list_pod_for_all_namespaces().items
    except ApiException as e:
        raise HTTPException(status_code=502, detail=f"Application inventory unavailable: {e.reason}")
    grouped = {}
    for kind_name, w in [("Deployment", x) for x in ds] + [("StatefulSet", x) for x in sts]:
        ns = w.metadata.namespace
        if ns in settings.excluded_namespaces: continue
        labels = w.metadata.labels or {}
        app_name = labels.get("app.kubernetes.io/name") or labels.get("app") or w.metadata.name
        key = ns + "/" + app_name
        item = grouped.setdefault(key, {"namespace": ns, "name": app_name, "workloads": [], "desired": 0, "ready": 0, "pods": 0})
        desired = w.spec.replicas or 0
        ready = w.status.ready_replicas or 0
        item["workloads"].append({"kind": kind_name, "name": w.metadata.name, "desired": desired, "ready": ready})
        item["desired"] += desired; item["ready"] += ready
    for p in pods:
        ns = p.metadata.namespace
        if ns in settings.excluded_namespaces: continue
        labels = p.metadata.labels or {}
        app_name = labels.get("app.kubernetes.io/name") or labels.get("app")
        if app_name and ns + "/" + app_name in grouped: grouped[ns + "/" + app_name]["pods"] += 1
    return sorted(grouped.values(), key=lambda x: (x["namespace"], x["name"]))

@router.get("/timeline")
def timeline(namespace: str | None = None, limit: int = 200):
    v1, _ = core()
    try:
        items = v1.list_namespaced_event(namespace).items if namespace else v1.list_event_for_all_namespaces().items
    except ApiException as e:
        raise HTTPException(status_code=502, detail=f"Event timeline unavailable: {e.reason}")
    out = []
    for e in items:
        ns = e.metadata.namespace or ""
        if ns in settings.excluded_namespaces: continue
        when = e.last_timestamp or e.event_time or e.first_timestamp or e.metadata.creation_timestamp
        out.append({"time": when.isoformat() if when else None, "namespace": ns,
                    "type": e.type or "Normal", "reason": e.reason or "Event",
                    "object": f"{e.involved_object.kind or 'Resource'}/{e.involved_object.name or ''}",
                    "message": e.message or "", "count": e.count or 1})
    out.sort(key=lambda x: x["time"] or "", reverse=True)
    return out[:min(max(limit, 1), 500)]

@router.get("/checks")
def checks():
    v1, apps = core()
    results = []
    try:
        nodes = v1.list_node().items
        pods = [p for p in v1.list_pod_for_all_namespaces().items if p.metadata.namespace not in settings.excluded_namespaces]
        deps = [d for d in apps.list_deployment_for_all_namespaces().items if d.metadata.namespace not in settings.excluded_namespaces]
    except ApiException as e:
        raise HTTPException(status_code=502, detail=f"Health checks unavailable: {e.reason}")
    for n in nodes:
        for c in n.status.conditions or []:
            if c.type == "Ready" or (c.type in ("MemoryPressure", "DiskPressure", "PIDPressure") and c.status == "True"):
                if c.status == "Unknown":
                    check_status = "WARN"
                elif c.type == "Ready":
                    check_status = "PASS" if c.status == "True" else "FAIL"
                else:
                    check_status = "FAIL" if c.status == "True" else "PASS"
                results.append({"status": check_status, "scope": "Node", "resource": n.metadata.name,
                                "check": c.type, "message": c.message or c.reason or c.status})
    for p in pods:
        phase = p.status.phase or "Unknown"
        if phase != "Running":
            results.append({"status": "FAIL" if phase == "Failed" else "WARN", "scope": "Pod",
                            "resource": p.metadata.namespace + "/" + p.metadata.name,
                            "check": "Pod phase", "message": phase})
        for c in p.status.container_statuses or []:
            state = c.state.waiting if c.state else None
            if state and state.reason:
                results.append({"status": "FAIL" if state.reason in ("CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull") else "WARN",
                                "scope": "Container", "resource": p.metadata.namespace + "/" + p.metadata.name + "/" + c.name,
                                "check": state.reason, "message": state.message or state.reason})
    for d in deps:
        desired = d.spec.replicas or 0; ready = d.status.ready_replicas or 0
        if ready < desired:
            results.append({"status": "WARN", "scope": "Deployment", "resource": d.metadata.namespace + "/" + d.metadata.name,
                            "check": "Replica availability", "message": f"{ready}/{desired} replicas ready"})
    results.sort(key=lambda x: (x["status"] != "FAIL", x["status"] != "WARN", x["scope"], x["resource"]))
    return {"summary": {s: sum(x["status"] == s for x in results) for s in ("PASS", "WARN", "FAIL")}, "checks": results}

@router.get("/capacity")
def capacity():
    v1, _ = core()
    from kubernetes.utils.quantity import parse_quantity
    def q(value):
        try: return float(parse_quantity(value or "0"))
        except Exception: return 0.0
    try:
        nodes = v1.list_node().items
        pods = [p for p in v1.list_pod_for_all_namespaces().items if p.metadata.namespace not in settings.excluded_namespaces]
    except ApiException as e:
        raise HTTPException(status_code=502, detail=f"Capacity data unavailable: {e.reason}")
    cpu_cap=mem_cap=cpu_req=mem_req=0.0
    for n in nodes:
        alloc=n.status.allocatable or {}
        cpu_cap += q(alloc.get("cpu")) * 1000
        mem_cap += q(alloc.get("memory"))
    for p in pods:
        for c in p.spec.containers or []:
            req=(c.resources.requests or {}) if c.resources else {}
            cpu_req += q(req.get("cpu")) * 1000
            mem_req += q(req.get("memory"))
    return {"nodes": len(nodes), "pods": len(pods), "cpu_millicores": {"requested": round(cpu_req), "allocatable": round(cpu_cap), "pct": round(cpu_req/cpu_cap*100,1) if cpu_cap else None},
            "memory_bytes": {"requested": round(mem_req), "allocatable": round(mem_cap), "pct": round(mem_req/mem_cap*100,1) if mem_cap else None},
            "note": "Requests are summed from container resource requests; limits and live usage are separate measures."}

@router.get("/cost")
def cost():
    data = capacity()
    return {"pricing_configured": False, "currency": None, "estimated_cost": None,
            "cpu_requested_millicores": data["cpu_millicores"]["requested"],
            "memory_requested_bytes": data["memory_bytes"]["requested"],
            "note": "No cloud/on-prem pricing model is configured. Showing resource consumption only; no monetary cost is fabricated."}

@router.get("/traffic")
async def traffic():
    if not settings.prometheus_url:
        return {"configured": False, "series": [], "note": "Configure PROMETHEUS_URL and suitable network metrics to enable traffic rates."}
    queries = {
        "receive_bytes_per_second": 'sum by (namespace,pod) (rate(container_network_receive_bytes_total{pod!=""}[5m]))',
        "transmit_bytes_per_second": 'sum by (namespace,pod) (rate(container_network_transmit_bytes_total{pod!=""}[5m]))'
    }
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            out = {}
            for key, expr in queries.items():
                r = await c.get(settings.prometheus_url.rstrip("/")+"/api/v1/query", params={"query":expr}); r.raise_for_status()
                out[key] = [{"metric": x.get("metric", {}), "value": float(x["value"][1])} for x in r.json().get("data",{}).get("result",[])]
        return {"configured": True, **out}
    except Exception:
        return {"configured": True, "series": [], "error": "Prometheus network metrics are unavailable or not enabled."}

@router.get("/helm")
def helm_releases():
    # Helm v3 stores release metadata in Secrets; deliberately do not request Secret access.
    # HelmRelease CRs from Flux are surfaced when installed.
    try:
        custom = k8s_client.custom_objects()
        items = custom.list_cluster_custom_object("helm.toolkit.fluxcd.io", "v2", "helmreleases").get("items", [])
        return {"source": "Flux HelmRelease", "available": True, "items": [
            {"namespace": x.get("metadata",{}).get("namespace"), "name": x.get("metadata",{}).get("name"),
             "chart": x.get("spec",{}).get("chart",{}).get("spec",{}).get("chart"),
             "version": x.get("spec",{}).get("chart",{}).get("spec",{}).get("version"),
             "ready": any(c.get("type") == "Ready" and c.get("status") == "True" for c in x.get("status",{}).get("conditions",[]))}
            for x in items]}
    except Exception:
        return {"source": "Flux HelmRelease", "available": False, "items": [],
                "note": "No readable Flux HelmRelease CRD found. Native Helm release Secrets are intentionally not accessed."}

@router.get("/gitops")
def gitops():
    custom = k8s_client.custom_objects()
    resources = [("argoproj.io", "v1alpha1", "applications", "Argo CD Application"),
                 ("source.toolkit.fluxcd.io", "v1", "gitrepositories", "Flux GitRepository"),
                 ("kustomize.toolkit.fluxcd.io", "v1", "kustomizations", "Flux Kustomization"),
                 ("helm.toolkit.fluxcd.io", "v2", "helmreleases", "Flux HelmRelease")]
    out=[]; found=False
    for group, version, plural, label in resources:
        try:
            data=custom.list_cluster_custom_object(group,version,plural)
            found=True
            for x in data.get("items",[]):
                meta=x.get("metadata",{}); status=x.get("status",{}); conditions=status.get("conditions",[])
                ready=next((c for c in conditions if c.get("type")=="Ready"),None)
                out.append({"provider":label,"namespace":meta.get("namespace","default"),"name":meta.get("name"),
                            "ready":ready.get("status")=="True" if ready else None,
                            "reason":ready.get("reason") if ready else "Status unavailable",
                            "message":ready.get("message") if ready else "", "revision":status.get("lastAppliedRevision") or status.get("lastAttemptedRevision") or status.get("sync" ,{}).get("revision")})
        except Exception:
            continue
    return {"available":found,"items":out,"note":None if found else "No supported Argo CD or Flux resources found, or the CRD is not readable."}
