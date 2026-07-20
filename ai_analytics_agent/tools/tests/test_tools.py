from decimal import Decimal

import pytest

from ai_analytics_agent.tools.query_engine import _format_result
from ai_analytics_agent.tools.sales_tools import get_sales_metric
from ai_analytics_agent.tools.wastage_tools import get_wastage_metric
from ai_analytics_agent.tools.flight_catalog_tools import get_flight_catalog_metric
from ai_analytics_agent.tools.product_catalog_tools import get_product_catalog_metric
from ai_analytics_agent.tools.pax_sales_tools import get_pax_sales_metric



def test_format_result_converts_decimal_to_float():
    result = _format_result([{"revenue": Decimal("123.45")}], row_limit=10)
    assert result["rows"] == [{"revenue": 123.45}]
    assert result["truncated"] is False


def test_format_result_truncates_when_over_limit():
    rows = [{"revenue": i} for i in range(5)]
    result = _format_result(rows, row_limit=3)
    assert len(result["rows"]) == 3
    assert result["truncated"] is True


def test_format_result_no_truncation_when_within_limit():
    rows = [{"revenue": i} for i in range(3)]
    result = _format_result(rows, row_limit=3)
    assert len(result["rows"]) == 3
    assert result["truncated"] is False


def test_format_result_force_not_truncated():
    rows = [{"revenue": i} for i in range(10)]
    result = _format_result(rows, row_limit=5, force_not_truncated=True)
    print(result)
    assert result["truncated"] is False
    assert len(result["rows"]) == 5


@pytest.mark.integration
def test_get_sales_metric_end_to_end():
    result = get_sales_metric(metrics=["revenue"], group_by=["year"])
    assert isinstance(result["rows"], list)
    assert "revenue" in result["rows"][0]

@pytest.mark.integration
def test_get_wastage_metric_end_to_end():
    result = get_wastage_metric(metrics=["loaded_quantity"], group_by=["month"])
    assert isinstance(result["rows"], list)
    assert "loaded_quantity" in result["rows"][0]

@pytest.mark.integration
def test_get_flight_catalog_metric_end_to_end():
    result = get_flight_catalog_metric(metrics=["flight_count"], group_by=["year"])
    assert isinstance(result["rows"], list)
    assert "flight_count" in result["rows"][0]

@pytest.mark.integration
def test_get_product_catalog_metric_end_to_end():
    result = get_product_catalog_metric(metrics=["item_count"], group_by=["category"])
    assert isinstance(result["rows"], list)
    assert "item_count" in result["rows"][0]

@pytest.mark.integration
def test_get_pax_sales_metric_end_to_end():
    result = get_pax_sales_metric(metrics=["avg_sales_per_passenger"], group_by=["year"])
    assert isinstance(result["rows"], list)
    assert "avg_sales_per_passenger" in result["rows"][0]