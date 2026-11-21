import asyncio, hashlib, json, logging, uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
from app.config import settings
from app.db import SessionLocal, Incident
from app.ai import analyze
from app.emailer import send_incident_email

log = logging.getLogger("monitor")
RCA_POOL = ThreadPoolExecutor(max_workers=4, thread_name_prefix="rca-worker")

def load_kube_config():
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()

def _fingerprint(kind, namespace, name, reason):
    return hashlib.sha256(f"{kind}|{namespace}|{name}|{reason}".encode()).hexdigest()

def _severity(reason):
    if reason in {"OOMKilled", "CrashLoopBackOff", "NodeNotReady", "FailedScheduling"}:
        return "HIGH"
    if reason in {"ImagePullBackOff", "ErrImagePull", "FailedMount", "Evicted"}:
        return "MEDIUM"
    return "LOW"

# Prevent duplicate RCA jobs within this application process.
RCA_JOBS = set()
RCA_JOBS_LOCK = __import__("threading").Lock()


def _as_utc(value):
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _schedule_rca(incident_id):
    with RCA_JOBS_LOCK:
        if incident_id in RCA_JOBS:
            return False
        RCA_JOBS.add(incident_id)

    def run():
        try:
            asyncio.run(_complete_incident(incident_id))
        finally:
            with RCA_JOBS_LOCK:
                RCA_JOBS.discard(incident_id)

    try:
        RCA_POOL.submit(run)
        return True
    except Exception:
        with RCA_JOBS_LOCK:
            RCA_JOBS.discard(incident_id)
        log.exception("Could not schedule RCA for incident %s", incident_id)
        return False


def _save_and_analyze(evidence):
    fp = _fingerprint(evidence["kind"], evidence.get("namespace", ""),
                      evidence.get("name", ""), evidence["reason"])

    with SessionLocal() as db:
        existing = db.query(Incident).filter_by(
            fingerprint=fp, status="OPEN"
        ).first()

        if existing:
            existing.occurrences += 1
            existing.updated_at = datetime.now(timezone.utc)
            incident_id = existing.id
            should_schedule = (
                existing.rca is None
                and existing.rca_status in {"PENDING", "FAILED"}
                and existing.rca_attempts < settings.rca_max_attempts
                and (
                    existing.rca_next_retry is None
                    or _as_utc(existing.rca_next_retry) <= datetime.now(timezone.utc)
                )
            )
            db.commit()
        else:
            incident = Incident(
                id=str(uuid.uuid4()),
                fingerprint=fp,
                severity=_severity(evidence["reason"]),
                namespace=evidence.get("namespace", ""),
                pod_name=evidence.get("name", ""),
                node_name=evidence.get("node", ""),
                summary=evidence.get("summary", evidence["reason"]),
                evidence=evidence,
                rca_status="PENDING",
                rca_attempts=0,
            )
            db.add(incident)
            db.commit()
            incident_id = incident.id
            should_schedule = True

    if should_schedule:
        _schedule_rca(incident_id)


def _retry_delay(attempt):
    delay = settings.rca_retry_base_seconds * (2 ** max(0, attempt - 1))
    return min(delay, settings.rca_retry_max_seconds)


async def _complete_incident(incident_id):
    with SessionLocal() as db:
        inc = db.get(Incident, incident_id)
        if not inc or inc.rca is not None:
            return
        if inc.rca_attempts >= settings.rca_max_attempts:
            inc.rca_status = "FAILED"
            inc.rca_next_retry = None
            db.commit()
            return

        inc.rca_status = "PROCESSING"
        inc.rca_attempts += 1
        inc.rca_next_retry = None
        attempt = inc.rca_attempts
        evidence = dict(inc.evidence)
        db.commit()

    log.info("Starting RCA for incident %s (attempt %s/%s)",
             incident_id, attempt, settings.rca_max_attempts)

    try:
        rca = await analyze(evidence)
        if not isinstance(rca, dict):
            raise ValueError("AI provider returned an invalid RCA object")

    except Exception as exc:
        with SessionLocal() as db:
            inc = db.get(Incident, incident_id)
            if inc:
                inc.rca_status = "FAILED"
                inc.rca_last_error = str(exc)[:2000]
                if inc.rca_attempts < settings.rca_max_attempts:
                    delay = _retry_delay(inc.rca_attempts)
                    inc.rca_next_retry = datetime.now(timezone.utc) + timedelta(seconds=delay)
                    log.warning(
                        "RCA failed for incident %s; retry %s scheduled in %s seconds: %s",
                        incident_id, inc.rca_attempts + 1, delay, exc
                    )
                else:
                    inc.rca_next_retry = None
                    log.error("RCA exhausted %s attempts for incident %s",
                              inc.rca_attempts, incident_id)
                db.commit()
        return

    with SessionLocal() as db:
        inc = db.get(Incident, incident_id)
        if not inc:
            return
        inc.rca = rca
        inc.rca_status = "COMPLETED"
        inc.rca_last_error = None
        inc.rca_next_retry = None
        db.commit()

    log.info("RCA completed for incident %s", incident_id)

    try:
        with SessionLocal() as db:
            inc = db.get(Incident, incident_id)
            if not inc:
                return
            email_status = await send_incident_email(inc, rca)

        with SessionLocal() as db:
            inc = db.get(Incident, incident_id)
            if inc:
                inc.email_status = email_status
                db.commit()
        log.info("Email processing for incident %s: %s", incident_id, email_status)

    except Exception:
        log.exception("Email delivery failed for incident %s", incident_id)
        with SessionLocal() as db:
            inc = db.get(Incident, incident_id)
            if inc:
                inc.email_status = "FAILED"
                db.commit()


def retry_pending_incidents():
    while True:
        try:
            with SessionLocal() as db:
                rows = db.query(Incident).filter(
                    Incident.status == "OPEN",
                    Incident.rca.is_(None),
                    Incident.rca_status.in_(["PENDING", "FAILED"]),
                    Incident.rca_attempts < settings.rca_max_attempts
                ).all()

                due_ids = [
                    row.id for row in rows
                    if row.rca_next_retry is None
                    or _as_utc(row.rca_next_retry) <= datetime.now(timezone.utc)
                ]

            for incident_id in due_ids:
                _schedule_rca(incident_id)

        except Exception:
            log.exception("RCA retry scheduler encountered an error")

        import time
        time.sleep(15)


def recover_pending_incidents():
    now = datetime.now(timezone.utc)

    with SessionLocal() as db:
        rows = db.query(Incident).filter(
            Incident.status == "OPEN",
            Incident.rca.is_(None),
            Incident.rca_status.in_(["PENDING", "PROCESSING", "FAILED"]),
            Incident.rca_attempts < settings.rca_max_attempts
        ).all()

        pending_ids = [
            row.id for row in rows
            if row.rca_status in {"PENDING", "PROCESSING"}
            or row.rca_next_retry is None
            or _as_utc(row.rca_next_retry) <= now
        ]

    log.info("Recovering %s eligible pending RCA incidents", len(pending_ids))
    for incident_id in pending_ids:
        _schedule_rca(incident_id)


def _pod_evidence(v1, pod):
    ns, name = pod.metadata.namespace, pod.metadata.name
    if ns in settings.excluded_namespaces:
        return None
    statuses = pod.status.container_statuses or []
    restarts = sum(x.restart_count or 0 for x in statuses)
    reasons = []
    for s in statuses:
        state = s.state
        waiting = getattr(state, "waiting", None)
        terminated = getattr(state, "terminated", None)
        last = s.last_state.terminated if s.last_state else None
        reason = (waiting.reason if waiting else None) or (terminated.reason if terminated else None) or (last.reason if last else None)
        if reason in {"CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull"}:
            reasons.append(reason)
        if last and last.reason == "OOMKilled":
            reasons.append("OOMKilled")
    phase = pod.status.phase or "Unknown"
    if phase == "Pending":
        age = (datetime.now(timezone.utc) - pod.metadata.creation_timestamp).total_seconds() if pod.metadata.creation_timestamp else 0
        if age >= settings.pending_minutes * 60:
            reasons.append("PendingTooLong")
    if restarts >= settings.restart_threshold:
        reasons.append("RepeatedRestarts")
    if phase == "Failed":
        reasons.append("PodFailed")
    if not reasons:
        return None
    reason = reasons[0]
    logs = []
    for s in statuses[:5]:
        try:
            logs.append(v1.read_namespaced_pod_log(name, ns, container=s.name,
                previous=True, tail_lines=settings.log_tail_lines))
        except ApiException:
            try:
                logs.append(v1.read_namespaced_pod_log(name, ns, container=s.name,
                    tail_lines=settings.log_tail_lines))
            except ApiException:
                pass
    events = []
    try:
        evs = v1.list_namespaced_event(ns, field_selector=f"involvedObject.name={name}", limit=40)
        events = [{"reason": e.reason, "message": e.message, "type": e.type,
                   "count": e.count, "lastTimestamp": str(e.last_timestamp or e.event_time)}
                  for e in evs.items]
    except ApiException:
        pass
    return {"kind":"Pod", "namespace":ns, "name":name,
        "node":pod.spec.node_name or "", "reason":reason,
        "summary":f"Pod {ns}/{name} phase={phase}; restarts={restarts}; detected={','.join(reasons)}",
        "phase":phase, "restart_count":restarts,
        "container_statuses":[{"name":s.name,"ready":s.ready,"restart_count":s.restart_count,
          "state":str(s.state),"last_state":str(s.last_state)} for s in statuses],
        "logs":"\n".join(logs)[:settings.max_log_chars], "events":events}

def pod_watch_loop():
    v1 = client.CoreV1Api()
    while True:
        w = watch.Watch()
        try:
            for event in w.stream(v1.list_pod_for_all_namespaces, timeout_seconds=300):
                pod = event["object"]
                if event["type"] == "DELETED":
                    continue
                ev = _pod_evidence(v1, pod)
                if ev:
                    _save_and_analyze(ev)
        except Exception:
            log.exception("Pod watch disconnected; reconnecting")
            import time; time.sleep(3)
        finally:
            w.stop()

def event_watch_loop():
    v1 = client.CoreV1Api()
    while True:
        w = watch.Watch()
        try:
            for event in w.stream(v1.list_event_for_all_namespaces, timeout_seconds=300):
                obj = event["object"]
                if obj.type != "Warning":
                    continue
                ns = obj.metadata.namespace or ""
                if ns in settings.excluded_namespaces:
                    continue
                reason = obj.reason or "WarningEvent"
                evidence = {"kind":"Event", "namespace":ns,
                    "name":obj.involved_object.name if obj.involved_object else "",
                    "node":obj.source.host if obj.source else "",
                    "reason":reason, "summary":obj.message or reason,
                    "events":[{"reason":reason,"message":obj.message,"type":obj.type,
                               "count":obj.count}]}
                _save_and_analyze(evidence)
        except Exception:
            log.exception("Event watch disconnected; reconnecting")
            import time; time.sleep(3)
        finally:
            w.stop()

def start_watchers():
    load_kube_config()
    import threading

    log.info("Starting Kubernetes pod and event watchers")
    threading.Thread(target=pod_watch_loop, daemon=True, name="pod-watcher").start()
    threading.Thread(target=event_watch_loop, daemon=True, name="event-watcher").start()
    threading.Thread(
        target=retry_pending_incidents,
        daemon=True,
        name="rca-retry-scheduler"
    ).start()
    recover_pending_incidents()
