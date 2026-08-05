from unittest.mock import patch

from ai_analytics_agent.tools.chart_builder import build_chart_spec

SEMANTIC_LAYER = {
    "dimensions": {
        "month_year": {"select": "dd.month_year", "requires": [], "time": True},
        "category": {"select": "dp.category", "requires": []},
        "route": {"select": "df.route", "requires": []},
    }
}


def test_build_chart_spec_returns_none_when_no_candidate():
    assert build_chart_spec(None) is None


@patch("ai_analytics_agent.tools.chart_builder.get_semantic_layer", return_value=SEMANTIC_LAYER)
def test_build_chart_spec_line_when_only_time_dim(mock_get_semantic_layer):
    candidate = {
        "domain": "sales",
        "metrics": ["revenue"],
        "group_by": ["month_year"],
        "rows": {"rows": [
            {"month_year": "01-2026", "revenue": 100},
            {"month_year": "02-2026", "revenue": 150},
        ], "truncated": False},
    }

    spec = build_chart_spec(candidate)

    assert spec == {
        "chart_type": "line",
        "x_key": "month_year",
        "series_keys": ["revenue"],
        "data": [
            {"month_year": "01-2026", "revenue": 100},
            {"month_year": "02-2026", "revenue": 150},
        ],
        "title": "Revenue by Month year",
    }


@patch("ai_analytics_agent.tools.chart_builder.get_semantic_layer", return_value=SEMANTIC_LAYER)
def test_build_chart_spec_bar_when_only_categorical_dim(mock_get_semantic_layer):
    candidate = {
        "domain": "sales",
        "metrics": ["revenue"],
        "group_by": ["category"],
        "rows": {"rows": [
            {"category": "Snacks", "revenue": 100},
            {"category": "Beverages", "revenue": 80},
        ], "truncated": False},
    }

    spec = build_chart_spec(candidate)

    assert spec == {
        "chart_type": "bar",
        "x_key": "category",
        "series_keys": ["revenue"],
        "data": [
            {"category": "Snacks", "revenue": 100},
            {"category": "Beverages", "revenue": 80},
        ],
        "title": "Revenue by Category",
    }


@patch("ai_analytics_agent.tools.chart_builder.get_semantic_layer", return_value=SEMANTIC_LAYER)
def test_build_chart_spec_pivots_when_time_and_categorical_dims(mock_get_semantic_layer):
    candidate = {
        "domain": "sales",
        "metrics": ["revenue"],
        "group_by": ["month_year", "category"],
        "rows": {"rows": [
            {"month_year": "01-2026", "category": "Snacks", "revenue": 100},
            {"month_year": "01-2026", "category": "Beverages", "revenue": 50},
            {"month_year": "02-2026", "category": "Snacks", "revenue": 120},
        ], "truncated": False},
    }

    spec = build_chart_spec(candidate)

    assert spec == {
        "chart_type": "bar",
        "x_key": "month_year",
        "series_keys": ["Snacks", "Beverages"],
        "data": [
            {"month_year": "01-2026", "Snacks": 100, "Beverages": 50},
            {"month_year": "02-2026", "Snacks": 120},
        ],
        "title": "Revenue by Category and Month year",
    }


@patch("ai_analytics_agent.tools.chart_builder.get_semantic_layer", return_value=SEMANTIC_LAYER)
def test_build_chart_spec_caps_to_first_metric_when_pivoting(mock_get_semantic_layer):
    candidate = {
        "domain": "sales",
        "metrics": ["revenue", "sales_quantity"],
        "group_by": ["month_year", "category"],
        "rows": {"rows": [
            {"month_year": "01-2026", "category": "Snacks", "revenue": 100, "sales_quantity": 10},
        ], "truncated": False},
    }

    spec = build_chart_spec(candidate)

    assert spec["series_keys"] == ["Snacks"]
    assert spec["data"] == [{"month_year": "01-2026", "Snacks": 100}]


@patch("ai_analytics_agent.tools.chart_builder.get_semantic_layer", return_value=SEMANTIC_LAYER)
def test_build_chart_spec_none_when_two_categorical_dims(mock_get_semantic_layer):
    candidate = {
        "domain": "sales",
        "metrics": ["revenue"],
        "group_by": ["category", "route"],
        "rows": {"rows": [{"category": "Snacks", "route": "LHR_JFK", "revenue": 100}], "truncated": False},
    }

    assert build_chart_spec(candidate) is None


@patch("ai_analytics_agent.tools.chart_builder.get_semantic_layer", return_value=SEMANTIC_LAYER)
def test_build_chart_spec_none_when_too_many_categories(mock_get_semantic_layer):
    rows = [{"category": f"cat-{i}", "revenue": i} for i in range(15)]
    candidate = {
        "domain": "sales",
        "metrics": ["revenue"],
        "group_by": ["category"],
        "rows": {"rows": rows, "truncated": False},
    }

    assert build_chart_spec(candidate) is None


def test_build_chart_spec_none_when_rows_empty():
    candidate = {
        "domain": "sales",
        "metrics": ["revenue"],
        "group_by": ["category"],
        "rows": {"rows": [], "truncated": False},
    }

    assert build_chart_spec(candidate) is None
