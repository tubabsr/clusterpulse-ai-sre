"""Builders for real kubernetes-client model objects (same shapes the API server returns)."""
from kubernetes import client as k

TS = None


def meta(name, ns, labels=None, owners=None):
    return k.V1ObjectMeta(name=name, namespace=ns, labels=labels, owner_references=owners)


def owner(kind, name):
    return k.V1OwnerReference(api_version="apps/v1", kind=kind, name=name, uid="uid-" + name, controller=True)


def namespace(name):
    return k.V1Namespace(metadata=k.V1ObjectMeta(name=name))


def pod(name, ns, labels=None, owner_ref=None, phase="Running", ready=True, waiting=None, restarts=0,
        volumes=None, node="node-1"):
    state = (k.V1ContainerState(waiting=k.V1ContainerStateWaiting(reason=waiting)) if waiting
             else k.V1ContainerState(running=k.V1ContainerStateRunning()))
    cs = k.V1ContainerStatus(name="app", ready=ready, restart_count=restarts, image="img", image_id="id", state=state)
    return k.V1Pod(
        metadata=meta(name, ns, labels, [owner_ref] if owner_ref else None),
        spec=k.V1PodSpec(containers=[k.V1Container(name="app", image="img")], node_name=node, volumes=volumes),
        status=k.V1PodStatus(phase=phase, container_statuses=[cs]))


def replicaset(name, ns, deployment):
    return k.V1ReplicaSet(metadata=meta(name, ns, owners=[owner("Deployment", deployment)]),
                          spec=k.V1ReplicaSetSpec(selector=k.V1LabelSelector(match_labels={}), replicas=1))


def deployment(name, ns, labels, replicas=1, ready=1, env_from_cm=None, volumes=None, sa=None, image="img:1"):
    container = k.V1Container(
        name="app", image=image,
        env_from=[k.V1EnvFromSource(config_map_ref=k.V1ConfigMapEnvSource(name=env_from_cm))] if env_from_cm else None)
    return k.V1Deployment(
        metadata=meta(name, ns, labels),
        spec=k.V1DeploymentSpec(
            replicas=replicas, selector=k.V1LabelSelector(match_labels=labels),
            template=k.V1PodTemplateSpec(metadata=k.V1ObjectMeta(labels=labels),
                                         spec=k.V1PodSpec(containers=[container], volumes=volumes,
                                                          service_account_name=sa))),
        status=k.V1DeploymentStatus(ready_replicas=ready))


def service(name, ns, selector, port=80, stype="ClusterIP"):
    return k.V1Service(metadata=meta(name, ns),
                       spec=k.V1ServiceSpec(selector=selector, type=stype, cluster_ip="10.0.0.1",
                                            ports=[k.V1ServicePort(port=port)]))


def pvc(name, ns, phase="Bound", size="5Gi"):
    return k.V1PersistentVolumeClaim(
        metadata=meta(name, ns),
        spec=k.V1PersistentVolumeClaimSpec(storage_class_name="local-path",
                                           resources=k.V1VolumeResourceRequirements(requests={"storage": size})),
        status=k.V1PersistentVolumeClaimStatus(phase=phase, capacity={"storage": size}))


def pvc_volume(claim):
    return k.V1Volume(name="data", persistent_volume_claim=k.V1PersistentVolumeClaimVolumeSource(claim_name=claim))


def configmap(name, ns, keys=3):
    return k.V1ConfigMap(metadata=meta(name, ns), data={f"k{i}": "v" for i in range(keys)})


def serviceaccount(name, ns):
    return k.V1ServiceAccount(metadata=meta(name, ns))


def ingress(name, ns, host, svc):
    backend = k.V1IngressBackend(service=k.V1IngressServiceBackend(name=svc, port=k.V1ServiceBackendPort(number=80)))
    path = k.V1HTTPIngressPath(path="/", path_type="Prefix", backend=backend)
    return k.V1Ingress(metadata=meta(name, ns),
                       spec=k.V1IngressSpec(rules=[k.V1IngressRule(host=host, http=k.V1HTTPIngressRuleValue(paths=[path]))]))


def ingressroute(name, ns, host, svc, middlewares=()):
    return {"metadata": {"name": name, "namespace": ns},
            "spec": {"entryPoints": ["websecure"], "tls": {"secretName": "x"},
                     "routes": [{"match": f"Host(`{host}`) && PathPrefix(`/`)", "kind": "Rule",
                                 "middlewares": [{"name": m} for m in middlewares],
                                 "services": [{"name": svc, "port": 8080}]}]}}


def middleware(name, ns, mtype="basicAuth"):
    return {"metadata": {"name": name, "namespace": ns}, "spec": {mtype: {}}}
