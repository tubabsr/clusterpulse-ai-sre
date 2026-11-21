"""Run the dashboard locally WITHOUT a cluster or database (demo data, real topology code).

    pip install -r requirements.txt
    python -m dev.mock_server          # http://127.0.0.1:8099
"""
import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pathlib import Path

from app import topology
from app.dashboard_api import router
from app.topology import Snapshot
from tests import factories as f

NS = "observability"


def demo_snapshot() -> Snapshot:
    pg, rca = {"app": "clusterpulse-postgres"}, {"app": "k8s-ai-rca-agent"}
    return Snapshot(
        namespaces=[f.namespace(NS), f.namespace("caretta"), f.namespace("monitoring")],
        deployments=[f.deployment("clusterpulse-postgres", NS, pg, volumes=[f.pvc_volume("clusterpulse-postgres-data")]),
                     f.deployment("k8s-ai-rca-agent", NS, rca, env_from_cm="k8s-ai-rca-config", sa="k8s-ai-rca-agent"),
                     f.deployment("caretta", "caretta", {"app": "caretta"}, replicas=2, ready=1)],
        replicasets=[f.replicaset("pg-1", NS, "clusterpulse-postgres"), f.replicaset("rca-1", NS, "k8s-ai-rca-agent"),
                     f.replicaset("car-1", "caretta", "caretta")],
        pods=[f.pod("clusterpulse-postgres-1-x", NS, pg, f.owner("ReplicaSet", "pg-1")),
              f.pod("k8s-ai-rca-agent-1-y", NS, rca, f.owner("ReplicaSet", "rca-1")),
              f.pod("caretta-1-a", "caretta", {"app": "caretta"}, f.owner("ReplicaSet", "car-1")),
              f.pod("caretta-1-b", "caretta", {"app": "caretta"}, f.owner("ReplicaSet", "car-1"),
                    ready=False, waiting="CrashLoopBackOff", restarts=7)],
        services=[f.service("clusterpulse-postgres", NS, pg, 5432), f.service("k8s-ai-rca-agent", NS, rca, 8080),
                  f.service("caretta", "caretta", {"app": "caretta"}, 7117)],
        pvcs=[f.pvc("clusterpulse-postgres-data", NS)],
        configmaps=[f.configmap("k8s-ai-rca-config", NS, 18)],
        serviceaccounts=[f.serviceaccount("k8s-ai-rca-agent", NS)],
        ingressroutes=[f.ingressroute("clusterpulse-https", NS, "clusterpulse.local", "k8s-ai-rca-agent", ["clusterpulse-basic-auth"]),
                       f.ingressroute("clusterpulse-http-redirect", NS, "clusterpulse.local", "k8s-ai-rca-agent", ["clusterpulse-https-redirect"])],
        middlewares=[f.middleware("clusterpulse-basic-auth", NS), f.middleware("clusterpulse-https-redirect", NS, "redirectScheme")],
    )


topology.fetch_snapshot = lambda namespace=None: _scoped(namespace)


def _scoped(ns):
    s = demo_snapshot()
    if ns:
        s.namespaces = [n for n in s.namespaces if n.metadata.name == ns]
        for a in ("pods", "services", "deployments", "replicasets", "pvcs", "configmaps", "serviceaccounts"):
            setattr(s, a, [o for o in getattr(s, a) if o.metadata.namespace == ns])
        for a in ("ingressroutes", "middlewares"):
            setattr(s, a, [o for o in getattr(s, a) if o["metadata"]["namespace"] == ns])
    return s


app = FastAPI()
HTML = (Path(__file__).parent.parent / "app" / "dashboard.html").read_text(encoding="utf-8")


@app.get("/", response_class=HTMLResponse)
def index():
    return HTML


@app.get("/api/namespaces")
def namespaces():
    return [{"name": n.metadata.name, "phase": "Active"} for n in demo_snapshot().namespaces]


@app.get("/api/pods")
def pods(limit: int = 1000):
    out = []
    for p in demo_snapshot().pods:
        text, health, meta = topology._pod_state(p)
        out.append({"namespace": p.metadata.namespace, "name": p.metadata.name, "phase": meta["phase"],
                    "ready": int(meta["ready"].split("/")[0]), "containers": 1, "restarts": meta["restarts"],
                    "reason": "" if health == "ok" else text, "node": "node-1", "labels": p.metadata.labels,
                    "container_names": ["app"]})
    return out


@app.get("/incidents")
def incidents(limit: int = 100):
    return []


@app.get("/api/{rest:path}")
def other(rest: str):
    return {}


# real topology routes win over the catch-all above
MOCK = {'/api/namespaces', '/api/pods'}
app.router.routes = [r for r in router.routes if getattr(r, 'path', '') not in MOCK] + app.router.routes

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8099)
