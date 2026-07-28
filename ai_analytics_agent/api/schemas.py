from typing import Optional
from pydantic import BaseModel

class AskRequest(BaseModel):
    question: str
    conversation_id: Optional[str] = None

class AskResponse(BaseModel):
    answer: str
    conversation_id: str

class DashboardMetadataResponse(BaseModel):
    metrics: dict[str, list[str]]
    dimensions: dict[str, list[str]]

class DashboardMetricsResponse(BaseModel):
    rows: list[dict]
    truncated: bool