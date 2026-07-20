import logging

from sqlalchemy import text

from ai_analytics_agent.utils.config_handler import ROW_LIMIT, get_semantic_layer, SALES_METRIC, WASTAGE_METRIC, \
    PAX_SALES_METRIC, FLIGHT_METRIC, PRODUCT_METRIC
from ai_analytics_agent.utils.database import get_engine
from ai_analytics_agent.utils.exceptions import ValidationError



def get_metric(metric_type: str, metrics: list[str], group_by: list[str] = None, filters: dict = None, order_by: dict = None, limit: int = None):

    logging.debug(f"CALLING {metric_type} METRICS")

    group_by = group_by or []
    filters = filters or {}
    order_by = order_by or {}
    user_requested_limit = limit is not None
    if limit is None:
        limit = ROW_LIMIT

    semantic_layer = get_semantic_layer(metric_type)
    _validate_args(metric_type, metrics, semantic_layer, group_by, filters, order_by, limit)

    from_clause = semantic_layer['from']

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

    order_by_clause = _build_order(order_by) if order_by else ""

    if user_requested_limit:
        limit_value = limit
    else:
        limit_value = ROW_LIMIT + 1

    sql = f"""
        SELECT {select_clause}
        FROM {from_clause}
        {join_clause}
        {where_clause}
        {group_by_clause}
        {order_by_clause}
        LIMIT {limit_value}
    """

    with get_engine().connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()

    print(sql)

    logging.debug(f"FETCHED FROM DB")
    return _format_result(rows)


def _validate_args(metric_type: str, metrics: list[str], semantic_layer: dict, group_by: list[str] = None, filters: dict = None, order_by: dict = None, limit: int = None):
    #check metric type only from allowed
    if not metric_type in [SALES_METRIC, WASTAGE_METRIC, PAX_SALES_METRIC, FLIGHT_METRIC, PRODUCT_METRIC]:
        raise ValidationError(f"Unknown metric type: {metric_type}")

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

    # check order_by
    order_by = order_by or {}
    allowed_ordering_values = ["asc", "desc"]
    for order_key, order_value in order_by.items():
        if order_key not in metrics and order_key not in group_by:
            raise ValidationError(f"Unknown order_by value: {order_key}")
        if order_value not in allowed_ordering_values:
            raise ValidationError(f"Unknown ordering statement: {order_value}")

    # check limit
    if limit is not None:
        if not 0 < limit <= ROW_LIMIT:
            raise ValidationError(f"Provided limit value {limit} doesn't match the criteria (from 0 to {ROW_LIMIT})")


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

def _build_order(order_by: dict) -> str:

    order_parts = [f"{key} {direction.upper()}" for key, direction in order_by.items()]
    return "ORDER BY " + ", ".join(order_parts)

def _format_result(rows: list[str], row_limit: int = ROW_LIMIT, force_not_truncated: bool = False) -> dict:
    truncated = (not force_not_truncated) and (len(rows) > row_limit)
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