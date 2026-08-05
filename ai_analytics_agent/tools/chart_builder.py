from ai_analytics_agent.utils.config_handler import get_semantic_layer

MAX_CHART_SERIES = 10


def _humanize(key: str) -> str:
    text = key.replace("_", " ")
    return text[:1].upper() + text[1:]


def build_chart_spec(chart_candidate: dict | None) -> dict | None:
    if not chart_candidate:
        return None

    domain = chart_candidate.get("domain")
    metrics = chart_candidate.get("metrics") or []
    group_by = chart_candidate.get("group_by") or []
    rows = (chart_candidate.get("rows") or {}).get("rows") or []

    if not domain or not metrics or not group_by or not rows:
        return None

    dimensions = get_semantic_layer(domain)["dimensions"]
    time_dims = [dim for dim in group_by if dimensions.get(dim, {}).get("time")]
    categorical_dims = [dim for dim in group_by if dim not in time_dims]

    if len(time_dims) > 1 or len(categorical_dims) > 1:
        return None

    if categorical_dims:
        return _build_categorical_chart(rows, metrics, time_dims, categorical_dims[0])

    return _build_time_series_chart(rows, metrics, time_dims[0])


def _build_time_series_chart(rows: list[dict], metrics: list[str], time_dim: str) -> dict:
    data = [{time_dim: row[time_dim], **{metric: row[metric] for metric in metrics}} for row in rows]
    title = f"{', '.join(_humanize(m) for m in metrics)} by {_humanize(time_dim)}"
    return {
        "chart_type": "line",
        "x_key": time_dim,
        "series_keys": metrics,
        "data": data,
        "title": title,
    }


def _build_categorical_chart(rows: list[dict], metrics: list[str], time_dims: list[str], category_dim: str) -> dict | None:
    distinct_categories = list(dict.fromkeys(str(row[category_dim]) for row in rows))
    if len(distinct_categories) > MAX_CHART_SERIES:
        return None

    metric = metrics[0]

    if not time_dims:
        data = [{category_dim: row[category_dim], metric: row[metric]} for row in rows]
        return {
            "chart_type": "bar",
            "x_key": category_dim,
            "series_keys": [metric],
            "data": data,
            "title": f"{_humanize(metric)} by {_humanize(category_dim)}",
        }

    time_dim = time_dims[0]
    pivoted: dict = {}
    order = []
    for row in rows:
        x_value = row[time_dim]
        if x_value not in pivoted:
            pivoted[x_value] = {time_dim: x_value}
            order.append(x_value)
        pivoted[x_value][str(row[category_dim])] = row[metric]

    return {
        "chart_type": "bar",
        "x_key": time_dim,
        "series_keys": distinct_categories,
        "data": [pivoted[x] for x in order],
        "title": f"{_humanize(metric)} by {_humanize(category_dim)} and {_humanize(time_dim)}",
    }
