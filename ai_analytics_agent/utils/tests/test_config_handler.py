import pytest

from ai_analytics_agent.utils.config_handler import (
    _validate_semantics,
    get_semantic_layer,
    return_config,
    SALES_METRIC,
    WASTAGE_METRIC,
    FLIGHT_METRIC,
    PRODUCT_METRIC,
    PAX_SALES_METRIC,
)
from ai_analytics_agent.utils.exceptions import ValidationError


@pytest.fixture
def valid_semantic_layer():
    return {
        "metrics": {
            "revenue": {"type": "simple", "sql": "SUM(fs.purchase_amount)"},
            "average_sale": {
                "type": "ratio",
                "numerator": "SUM(fs.purchase_amount)",
                "denominator": "COUNT(DISTINCT fs.slip_id)",
            },
        },
        "dimensions": {
            "year": {"select": "dd.year", "requires": ["dim_date"]},
        },
        "joins": {
            "dim_date": "JOIN mart.dim_date dd ON dd.date_key = fs.date",
        },
        "join_order": ["dim_date"],
    }


def test_validate_semantics_passes_on_valid_layer(valid_semantic_layer):
    _validate_semantics(valid_semantic_layer)


def test_validate_semantics_raises_on_unknown_metric_type(valid_semantic_layer):
    valid_semantic_layer["metrics"]["broken"] = {"type": "unknown"}
    with pytest.raises(ValidationError):
        _validate_semantics(valid_semantic_layer)


def test_validate_semantics_raises_when_simple_metric_missing_sql(valid_semantic_layer):
    valid_semantic_layer["metrics"]["broken"] = {"type": "simple"}
    with pytest.raises(ValidationError):
        _validate_semantics(valid_semantic_layer)


def test_validate_semantics_raises_when_ratio_metric_missing_denominator(valid_semantic_layer):
    valid_semantic_layer["metrics"]["broken"] = {"type": "ratio", "numerator": "SUM(x)"}
    with pytest.raises(ValidationError):
        _validate_semantics(valid_semantic_layer)


def test_validate_semantics_raises_when_dimension_missing_select(valid_semantic_layer):
    valid_semantic_layer["dimensions"]["month"] = {"requires": ["dim_date"]}
    with pytest.raises(ValidationError):
        _validate_semantics(valid_semantic_layer)


def test_validate_semantics_raises_when_dimension_missing_requires(valid_semantic_layer):
    valid_semantic_layer["dimensions"]["month"] = {"select": "dd.month"}
    with pytest.raises(ValidationError):
        _validate_semantics(valid_semantic_layer)


def test_validate_semantics_raises_on_unknown_join_in_dimension(valid_semantic_layer):
    valid_semantic_layer["dimensions"]["month"] = {"select": "dd.month", "requires": ["not_a_join"]}
    with pytest.raises(ValidationError):
        _validate_semantics(valid_semantic_layer)


def test_validate_semantics_raises_when_join_order_missing(valid_semantic_layer):
    del valid_semantic_layer["join_order"]
    with pytest.raises(ValidationError):
        _validate_semantics(valid_semantic_layer)


def test_validate_semantics_raises_when_join_missing_from_join_order(valid_semantic_layer):
    valid_semantic_layer["joins"]["dim_time"] = "JOIN mart.dim_time dt ON dt.time_key = fs.time"
    with pytest.raises(ValidationError):
        _validate_semantics(valid_semantic_layer)


def test_validate_semantics_raises_when_join_order_references_unknown_join(valid_semantic_layer):
    valid_semantic_layer["join_order"].append("not_a_join")
    with pytest.raises(ValidationError):
        _validate_semantics(valid_semantic_layer)


def test_get_semantic_layer_raises_on_unknown_domain():
    with pytest.raises(ValidationError):
        get_semantic_layer("not_a_real_domain")


@pytest.mark.parametrize(
    "domain", [SALES_METRIC, WASTAGE_METRIC, FLIGHT_METRIC, PRODUCT_METRIC, PAX_SALES_METRIC]
)
def test_get_semantic_layer_returns_valid_layer_for_each_domain(domain):
    semantic_layer = get_semantic_layer(domain)
    assert "metrics" in semantic_layer
    assert "dimensions" in semantic_layer
    assert "join_order" in semantic_layer


def test_return_config_reads_real_sales_config():
    config = return_config(SALES_METRIC)
    assert "revenue" in config["metrics"]
