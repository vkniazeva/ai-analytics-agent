from typing import Literal, Optional
from pydantic import BaseModel

class AskRequest(BaseModel):
    question: str
    conversation_id: Optional[str] = None

class ChartSpec(BaseModel):
    chart_type: Literal["bar", "line"]
    x_key: str
    series_keys: list[str]
    data: list[dict]
    title: str

class AskResponse(BaseModel):
    answer: str
    conversation_id: str
    chart: Optional[ChartSpec] = None