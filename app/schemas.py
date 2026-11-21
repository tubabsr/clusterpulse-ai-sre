from pydantic import BaseModel, ConfigDict, Field, field_validator
from typing import Any

class CloseIncidentRequest(BaseModel):
    remark: str = Field(min_length=3, max_length=5000)

    @field_validator("remark")
    @classmethod
    def validate_remark(cls, value: str) -> str:
        value = value.strip()
        if len(value) < 3:
            raise ValueError("Closure remark must contain at least 3 non-whitespace characters")
        return value

class IncidentOut(BaseModel):
    id: str
    severity: str
    status: str
    namespace: str
    pod_name: str
    node_name: str
    summary: str
    rca: dict[str, Any] | None
    rca_status: str
    rca_attempts: int
    rca_next_retry: Any | None
    rca_last_error: str | None
    email_status: str
    occurrences: int
    created_at: Any | None = None
    closed_at: Any | None = None
    closure_remark: str | None = None
    model_config = ConfigDict(from_attributes=True)
