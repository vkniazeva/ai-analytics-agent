from sqlalchemy import text

from ai_analytics_agent.utils.config_handler import get_semantic_layer
from ai_analytics_agent.utils.database import get_engine
from ai_analytics_agent.utils.exceptions import ValidationError

ROW_LIMIT = 200

def get_sales_metric(metrics: list[str], group_by: list[str] = None, filters: dict = None):
    group_by = group_by or []
    filters = filters or {}

    semantic_layer = get_semantic_layer()
    _validate_args(metrics, semantic_layer, group_by, filters)

    metric_parts = [_build_metric_sql(metric, semantic_layer) for metric in metrics]
    dimension_parts = [
        f"{semantic_layer['dimensions'][dim]['select']} as {dim}" for dim in group_by
    ]
    select_clause = ", ".join(dimension_parts + metric_parts)

    join_clause = _resolve_joins(semantic_layer, group_by, filters)
    where_clause, params = _build_where(semantic_layer, filters)

    group_by_clause = ""
    if group_by:
        group_by_columns = [semantic_layer["dimensions"][dim]["select"] for dim in group_by]
        group_by_clause = "GROUP BY " + ", ".join(group_by_columns)

    sql = f"""
        SELECT {select_clause}
        FROM mart.fact_sales fs
        {join_clause}
        {where_clause}
        {group_by_clause}
        LIMIT {ROW_LIMIT + 1}
    """

    with get_engine().connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()

    return _format_result(rows)


def _validate_args(metrics: list[str], semantic_layer: dict, group_by: list[str] = None, filters: dict = None):
    # check that at least one metric is given
    if not metrics:
        raise ValidationError("Incorrect request. No metrics are given")

    # check that all metrics exist in semantic_layer
    for metric in metrics:
        if metric not in semantic_layer["metrics"]:
            raise ValidationError(f"Unknown metric name: {metric}")

    # check group_by is a list and exist in dimensions
    if group_by:
        if type(group_by) is not list:
            raise ValidationError(f"Provided group_by: {group_by} format is not list")

        for group in group_by:
            if group not in semantic_layer["dimensions"]:
                raise ValidationError(f"Unknown group_by clause: {group}")

    # check filters is dictionary and  exist in dimensions
    if filters:
        if type(filters) is not dict:
            raise ValidationError(f"Provided filters: {filters} format is not dict")

        for f in filters:
            if f not in semantic_layer["dimensions"]:
                raise ValidationError(f"Unknown filter clause: {f}")

    # check grou_by and filters join order


def _build_metric_sql(metric: str, semantic_layer: dict) -> str:

    # build SELECT part from SQL query
    metric_def = semantic_layer["metrics"][metric]
    if metric_def["type"] == "simple":
        return f"{metric_def['sql']} as {metric}"
    elif metric_def["type"] == "ratio":
        return (f"{metric_def['numerator']}::numeric "
                f"/ nullif({metric_def['denominator']}, 0) as {metric}")
    else:
        raise ValidationError("Unknown metric type")


def _resolve_joins(semantic_layer: dict, group_by: list[str] = None, filters: dict = None):
    group_by = group_by or []
    filters = filters or {}

    all_dims = group_by + list(filters.keys())

    required_tables = set()
    for dim in all_dims:
        required_tables.update(semantic_layer["dimensions"][dim]["requires"])

    join_order = semantic_layer["join_order"]
    ordered_tables = [table for table in join_order if table in required_tables]

    join_clause = " ".join(semantic_layer["joins"][table] for table in ordered_tables)
    return join_clause

def _build_where(semantic_layer: dict, filters: dict = None)-> tuple[str, dict]:
    filters = filters or {}

    if not filters:
        return "", {}

    where_parts = []
    params = {}

    for name, value in filters.items():
        column = semantic_layer["dimensions"][name]["select"]
        where_parts.append(f"{column} = :{name}")
        params[name] = value

    where_clause = "WHERE " + " AND ".join(where_parts)
    return where_clause, params

def _format_result(rows: list[str], row_limit: int = ROW_LIMIT) -> dict:
    truncated = len(rows) > row_limit
    trimmed_rows = rows[:row_limit]

    formatted_rows = []
    for row in trimmed_rows:
        row_dict = dict(row)
        for key, value in row_dict.items():
            if hasattr(value, "__float__"):
                row_dict[key] = float(value)
        formatted_rows.append(row_dict)

    return {
        "rows": formatted_rows,
        "truncated": truncated,
    }