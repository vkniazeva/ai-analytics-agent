import uuid

from fastapi import FastAPI, HTTPException, Request
from ai_analytics_agent.api.schemas import AskRequest, AskResponse
from ai_analytics_agent.llm.agent_loop import run_agent

app = FastAPI()

CONVERSATIONS:dict[str, list[dict]] = {}

@app.post("/ask", response_model=AskResponse)
def ask_model(request: AskRequest):
    history = []
    if request.conversation_id and request.conversation_id in CONVERSATIONS:
        conversation_id = request.conversation_id
        history = CONVERSATIONS.get(request.conversation_id)
    else:
        conversation_id = str(uuid.uuid4())

    history.append({"role": "user", "content": request.question})

    answer, updated_messages = run_agent(history)
    CONVERSATIONS[conversation_id] = updated_messages
    return AskResponse(answer=answer, conversation_id=conversation_id)



