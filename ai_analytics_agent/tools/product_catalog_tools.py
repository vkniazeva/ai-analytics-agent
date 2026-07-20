from ai_analytics_agent.tools.query_engine import get_metric
from ai_analytics_agent.utils.config_handler import PRODUCT_METRIC


def get_product_catalog_metric(metrics: list[str], group_by: list[str] = None, filters: dict = None, order_by: dict = None, limit: int = None):
    return get_metric(PRODUCT_METRIC, metrics, group_by, filters, order_by, limit)