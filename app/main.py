import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.db import init_db, SessionLocal, Incident, engine
from app.monitor import start_watchers
from app.schemas import IncidentOut, CloseIncidentRequest
from fastapi.responses import HTMLResponse
from pathlib import Path
from app.dashboard_api import router as dashboard_router

logging.basicConfig(level=logging.INFO)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    start_watchers()
    yield

app = FastAPI(title="Kubernetes AI RCA Agent", version="0.3.0", lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1024)
app.include_router(dashboard_router)

# The dashboard is one static file: read it once instead of on every request.
_DASHBOARD_HTML = (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")

# The dashboard uses inline <script>/<style> and talks only to its own origin.
_CSP = ("default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")

@app.middleware("http")
async def security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    response.headers.setdefault("Content-Security-Policy", _CSP)
    if request.url.path.startswith(("/api/", "/incidents")) or request.url.path == "/":
        response.headers["Cache-Control"] = "no-store"  # live data / always-fresh shell
    return response

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard():
    return _DASHBOARD_HTML

@app.get("/healthz")
def health():
    # liveness: the process is up. Deliberately does NOT touch the DB, otherwise a DB blip
    # would make Kubernetes restart a perfectly healthy pod.
    return {"status":"ok"}

@app.get("/readyz")
def ready():
    # readiness: can we serve incidents? (stops routing traffic during a DB outage)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:
        logging.exception("Readiness check failed: database unreachable")
        return JSONResponse({"status": "not-ready", "reason": "database unreachable"}, status_code=503)
    return {"status":"ready"}

@app.get("/incidents", response_model=list[IncidentOut])
def incidents(limit: int = 100, db: Session = Depends(get_db)):
    return db.query(Incident).order_by(Incident.created_at.desc()).limit(min(limit, 500)).all()

@app.get("/incidents/{incident_id}", response_model=IncidentOut)
def incident_detail(incident_id: str, db: Session = Depends(get_db)):
    item = db.get(Incident, incident_id)
    if not item:
        raise HTTPException(status_code=404, detail="Incident not found")
    return item

@app.post("/incidents/{incident_id}/close", response_model=IncidentOut)
def close_incident(incident_id: str, payload: CloseIncidentRequest, db: Session = Depends(get_db)):
    item = db.get(Incident, incident_id)

    if not item:
        raise HTTPException(status_code=404, detail="Incident not found")

    if item.status == "CLOSED":
        return item

    from datetime import datetime, timezone
    item.status = "CLOSED"
    item.closure_remark = payload.remark.strip()
    item.closed_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(item)

    logging.info("Incident %s closed by operator", incident_id)
    return item
