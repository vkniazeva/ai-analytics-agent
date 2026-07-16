from typing import Optional
from pydantic import BaseModel

class AskRequest(BaseModel):
    question: str
    conversation_id: Optional[str] = None

class AskResponse(BaseModel):
    answer: str
    conversation_id: str