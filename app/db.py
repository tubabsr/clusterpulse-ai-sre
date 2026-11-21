from sqlalchemy import create_engine, String, Text, DateTime, Integer, JSON, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from app.config import settings

engine = create_engine(settings.database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

class Incident(Base):
    __tablename__ = "incidents"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    fingerprint: Mapped[str] = mapped_column(String(512), index=True)
    severity: Mapped[str] = mapped_column(String(16), index=True)
    status: Mapped[str] = mapped_column(String(24), default="OPEN")
    namespace: Mapped[str] = mapped_column(String(253), default="")
    pod_name: Mapped[str] = mapped_column(String(253), default="")
    node_name: Mapped[str] = mapped_column(String(253), default="")
    summary: Mapped[str] = mapped_column(Text, default="")
    evidence: Mapped[dict] = mapped_column(JSON, default=dict)
    rca: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    email_status: Mapped[str] = mapped_column(String(24), default="PENDING")
    rca_status: Mapped[str] = mapped_column(String(24), default="PENDING")
    rca_attempts: Mapped[int] = mapped_column(Integer, default=0)
    rca_next_retry: Mapped[object | None] = mapped_column(DateTime(timezone=True), nullable=True)
    rca_last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    occurrences: Mapped[int] = mapped_column(Integer, default=1)
    closure_remark: Mapped[str | None] = mapped_column(Text, nullable=True)
    closed_at: Mapped[object | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[object] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

def init_db():
    Base.metadata.create_all(engine)

    column_definitions = {
        "rca_status": "VARCHAR(24) NOT NULL DEFAULT 'PENDING'",
        "rca_attempts": "INTEGER NOT NULL DEFAULT 0",
        "rca_next_retry": "TIMESTAMP WITH TIME ZONE NULL",
        "rca_last_error": "TEXT NULL",
        "closure_remark": "TEXT NULL",
        "closed_at": "TIMESTAMP WITH TIME ZONE NULL",
    }

    with engine.begin() as conn:
        existing_columns = {
            column["name"]
            for column in __import__("sqlalchemy").inspect(conn).get_columns("incidents")
        }

        for name, definition in column_definitions.items():
            if name not in existing_columns:
                conn.exec_driver_sql(
                    f"ALTER TABLE incidents ADD COLUMN {name} {definition}"
                )

        conn.exec_driver_sql("""
            UPDATE incidents
            SET rca_status = CASE
                WHEN rca IS NOT NULL THEN 'COMPLETED'
                ELSE 'PENDING'
            END
            WHERE rca_status = 'PENDING'
        """)
