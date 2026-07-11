from pathlib import Path
from ai_analitics_agent.utils.exceptions import ValidationError
import yaml

SEMANTICS_PATH = Path(__file__).parent.parent

def return_config():

    config_path = SEMANTICS_PATH / "configs" / "semantic_layer.yaml"

    with open(config_path, "r") as f:
        semantic_layer = yaml.safe_load(f)
    return semantic_layer

def validate_semantics(semantic_layer):

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


def get_semantic_layer():
    semantic_layer = return_config()
    validate_semantics(semantic_layer)
    return semantic_layer


