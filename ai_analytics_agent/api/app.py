import uuid

from fastapi import FastAPI, HTTPException, Request, Query
from ai_analytics_agent.api.schemas import (
    AskRequest, AskResponse, DashboardMetadataResponse, DashboardMetricsResponse,
)
from ai_analytics_agent.llm.agent_loop import run_agent
from ai_analytics_agent.tools.query_engine import get_metric
from ai_analytics_agent.utils.config_handler import (
    get_semantic_layer, SALES_METRIC, WASTAGE_METRIC, FLIGHT_METRIC,
    PRODUCT_METRIC, PAX_SALES_METRIC,
)
from ai_analytics_agent.utils.exceptions import ValidationError
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

CONVERSATIONS:dict[str, list[dict]] = {}

DASHBOARD_DOMAINS = [SALES_METRIC, WASTAGE_METRIC, FLIGHT_METRIC, PRODUCT_METRIC, PAX_SALES_METRIC]

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

    answer, updated_messages = run_agent(history)
    CONVERSATIONS[conversation_id] = updated_messages
    return AskResponse(answer=answer, conversation_id=conversation_id)


@app.get("/dashboard/metadata", response_model=DashboardMetadataResponse)
def dashboard_metadata():
    metrics = {}
    dimensions = {}
    for domain in DASHBOARD_DOMAINS:
        semantic_layer = get_semantic_layer(domain)
        metrics[domain] = list(semantic_layer["metrics"].keys())
        dimensions[domain] = list(semantic_layer["dimensions"].keys())
    return DashboardMetadataResponse(metrics=metrics, dimensions=dimensions)


@app.get("/dashboard/metrics", response_model=DashboardMetricsResponse)
def dashboard_metrics(
    domain: str,
    metrics: list[str] = Query(...),
    group_by: list[str] = Query(default=[]),
):
    try:
        result = get_metric(domain, metrics, group_by)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return DashboardMetricsResponse(**result)



