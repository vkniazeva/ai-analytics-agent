import uuid

from fastapi import FastAPI
from ai_analytics_agent.api.schemas import AskRequest, AskResponse
from ai_analytics_agent.llm.agent_loop import run_agent
from ai_analytics_agent.tools.chart_builder import build_chart_spec
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

CONVERSATIONS:dict[str, list[dict]] = {}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/ask", response_model=AskResponse)
def ask_model(request: AskRequest):
    history = []
    if request.conversation_id and request.conversation_id in CONVERSATIONS:
        conversation_id = request.conversation_id
        history = CONVERSATIONS.get(request.conversation_id)
    else:
        conversation_id = str(uuid.uuid4())

    history.append({"role": "user", "content": request.question})

    answer, updated_messages, chart_candidate = run_agent(history)
    CONVERSATIONS[conversation_id] = updated_messages
    chart = build_chart_spec(chart_candidate)
    return AskResponse(answer=answer, conversation_id=conversation_id, chart=chart)
