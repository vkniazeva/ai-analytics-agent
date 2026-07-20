from ai_analytics_agent.tools.query_engine import get_metric
from ai_analytics_agent.utils.config_handler import FLIGHT_METRIC


def get_flight_catalog_metric(metrics: list[str], group_by: list[str] = None, filters: dict = None, order_by: dict = None, limit: int = None):
    return get_metric(FLIGHT_METRIC, metrics, group_by, filters, order_by, limit)