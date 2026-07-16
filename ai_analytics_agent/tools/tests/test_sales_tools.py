import pytest
from ai_analytics_agent.tools.sales_tools import _validate_args, _build_metric_sql, _resolve_joins, _build_where, \
    ROW_LIMIT, _build_order, _format_result
from ai_analytics_agent.utils.exceptions import ValidationError

@pytest.fixture
def semantic_layer():
    return {
        "metrics": {
            "revenue": {"type": "simple", "sql": "SUM(fs.purchase_amount)"},
            "average_items_in_slip": {"type": "ratio", "numerator": "SUM(fs.sold_quantity)", "denominator": "COUNT(DISTINCT fs.slip_id)"},
        },
        "dimensions": {
            "year": {"select": "dd.year", "requires": ["dim_flights", "dim_date"]},
            "flight_number": {"select": "df.flight_number", "requires": ["dim_flights"]},
        },
        "joins": {
            "dim_flights": "JOIN mart.dim_flights df ON df.flight_key = fs.flight_key",
            "dim_date": "JOIN mart.dim_date dd ON dd.date_key = df.date",
        },
        "join_order": ["dim_flights", "dim_date"],
    }

def test_validate_args_raises_when_no_metrics(semantic_layer):
    with pytest.raises(ValidationError):
        _validate_args(metrics=[], semantic_layer=semantic_layer)

def test_validate_args_raises_on_unknown_metric(semantic_layer):
    with pytest.raises(ValidationError):
        _validate_args(metrics=['not_real'], semantic_layer=semantic_layer)

def test_validate_args_passes_on_valid_metric(semantic_layer):
    _validate_args(metrics=["revenue"], semantic_layer=semantic_layer)

def test_validate_args_raises_on_unknown_group_by(semantic_layer):
    with pytest.raises(ValidationError):
        _validate_args(metrics=["revenue"], semantic_layer=semantic_layer, group_by=["not_real_dim"])

def test_validate_args_raises_when_group_by_not_list(semantic_layer):
    with pytest.raises(ValidationError):
        _validate_args(metrics=["revenue"], semantic_layer=semantic_layer, group_by="year")

def test_validate_args_raises_on_unknown_filter(semantic_layer):
    with pytest.raises(ValidationError):
        _validate_args(metrics=["revenue"], semantic_layer=semantic_layer, filters={"not_real": 2025})

def test_simple_metric_valid(semantic_layer):
    result = _build_metric_sql(metric="revenue", semantic_layer=semantic_layer)
    assert result == "SUM(fs.purchase_amount) as revenue"

def test_ratio_metric_valid(semantic_layer):
    result = _build_metric_sql(metric="average_items_in_slip", semantic_layer=semantic_layer)
    assert result == "SUM(fs.sold_quantity)::numeric / nullif(COUNT(DISTINCT fs.slip_id), 0) as average_items_in_slip"

def test_ratio_invalid_metric(semantic_layer):
    semantic_layer["metrics"]["broken_metric"] = {"type": "unknown", "sql": "broken_metric  "}
    with pytest.raises(ValidationError):
        _build_metric_sql(metric="broken_metric", semantic_layer=semantic_layer)

def test_join_group_by_valid(semantic_layer):
    result = _resolve_joins(semantic_layer=semantic_layer, group_by=["year"])
    assert result.count("JOIN") == 2
    assert result.index("dim_flights") < result.index("dim_date")

def test_join_group_by_duplicates_valid(semantic_layer):
    result = _resolve_joins(semantic_layer=semantic_layer, group_by=["year", "flight_number"])
    assert result.count("JOIN") == 2
    assert result.index("dim_flights") < result.index("dim_date")

def test_join_group_by_filter_none_valid(semantic_layer):
    result = _resolve_joins(semantic_layer=semantic_layer, group_by=None, filters=None)
    assert result == ""

def test_join_filters_no_group_by_valid(semantic_layer):
    result = _resolve_joins(semantic_layer=semantic_layer, group_by=None, filters = {"year": 2025} )
    assert result.count("JOIN") == 2
    assert result.index("dim_flights") < result.index("dim_date")

def test_build_where_single_filter(semantic_layer):
    where_clause, params = _build_where(semantic_layer=semantic_layer, filters={"year": 2025})
    assert where_clause == "WHERE dd.year = :year"
    assert params == {"year": 2025}

def test_build_where_multiple_filters(semantic_layer):
    where_clause, params = _build_where(
        semantic_layer=semantic_layer,
        filters={"year": 2025, "flight_number": "AB015"},
    )
    assert where_clause == "WHERE dd.year = :year AND df.flight_number = :flight_number"
    assert params == {"year": 2025, "flight_number": "AB015"}

def test_build_where_no_filters(semantic_layer):
    where_clause, params = _build_where(semantic_layer=semantic_layer, filters=None)
    assert where_clause == ""
    assert params == {}

def test_validate_args_order_by_valid(semantic_layer):
    _validate_args(
        metrics=["revenue"], semantic_layer=semantic_layer,
        group_by=["year"], order_by={"revenue": "desc"},
    )  # не должно упасть


def test_validate_args_order_by_unknown_key(semantic_layer):
    with pytest.raises(ValidationError):
        _validate_args(
            metrics=["revenue"], semantic_layer=semantic_layer,
            group_by=["year"], order_by={"not_requested": "desc"},
        )


def test_validate_args_order_by_bad_direction(semantic_layer):
    with pytest.raises(ValidationError):
        _validate_args(
            metrics=["revenue"], semantic_layer=semantic_layer,
            order_by={"revenue": "sideways"},
        )


def test_validate_args_limit_out_of_range(semantic_layer):
    with pytest.raises(ValidationError):
        _validate_args(metrics=["revenue"], semantic_layer=semantic_layer, limit=0)

    with pytest.raises(ValidationError):
        _validate_args(metrics=["revenue"], semantic_layer=semantic_layer, limit=ROW_LIMIT + 1)


def test_validate_args_limit_valid(semantic_layer):
    _validate_args(metrics=["revenue"], semantic_layer=semantic_layer, limit=10)


def test_build_order_single_key():
    assert _build_order({"revenue": "desc"}) == "ORDER BY revenue DESC"


def test_build_order_multiple_keys():
    result = _build_order({"revenue": "desc", "year": "asc"})
    assert result == "ORDER BY revenue DESC, year ASC"

