from pathlib import Path
from ai_analytics_agent.utils.exceptions import ValidationError
import yaml

SEMANTICS_PATH = Path(__file__).parent.parent

ROW_LIMIT = 200
SALES_METRIC = "sales"
WASTAGE_METRIC = "wastage"
FLIGHT_METRIC = "flights"
PRODUCT_METRIC = "product_catalog"
PAX_SALES_METRIC = "pax_sales"

def return_config(domain: str):

    config_path = SEMANTICS_PATH / "configs" / f"{domain}_semantic_layer.yaml"

    with open(config_path, "r") as f:
        semantic_layer = yaml.safe_load(f)
    return semantic_layer

def _validate_semantics(semantic_layer):

    metric_types = ["simple", "ratio"]

    metric_required_fields = {
        "simple": ["sql"],
        "ratio": ["numerator", "denominator"],
    }

    for metric, metric_def in semantic_layer["metrics"].items():
        metric_type = metric_def.get("type")

        if metric_type not in metric_types:
            raise ValidationError(f"Unknown type '{metric_type}' for metric '{metric}'")

        for field in metric_required_fields[metric_type]:
            if field not in metric_def:
                raise ValidationError(f"Metric '{metric}' is missing required field '{field}'")

    dimension_required_fields = ["select", "requires"]
    join_keys = semantic_layer["joins"].keys()

    for dimension, dim_def in semantic_layer["dimensions"].items():
        for field in dimension_required_fields:
            if field not in dim_def:
                raise ValidationError(f"Dimension '{dimension}' is missing required field '{field}'")

        for table in dim_def["requires"]:
            if table not in join_keys:
                raise ValidationError(
                    f"Dimension '{dimension}' requires unknown join '{table}'"
                )

    join_order = semantic_layer.get("join_order")
    if join_order is None:
        raise ValidationError("Missing 'join_order' section in semantic layer")

    for table in join_keys:
        if table not in join_order:
            raise ValidationError(f"Join '{table}' is missing from 'join_order'")

    for table in join_order:
        if table not in join_keys:
            raise ValidationError(f"'join_order' references unknown join '{table}'")


def get_semantic_layer(domain: str):
    if domain not in (SALES_METRIC, WASTAGE_METRIC, PRODUCT_METRIC, FLIGHT_METRIC, PAX_SALES_METRIC):
        raise ValidationError(f"Unknown metric type: {domain}")
    semantic_layer = return_config(domain)
    _validate_semantics(semantic_layer)
    return semantic_layer


