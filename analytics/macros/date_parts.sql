--key
{% macro date_to_key(date_column) %}
    {{ return(adapter.dispatch('date_to_key')(date_column)) }}
{% endmacro %}

{% macro postgres__date_to_key(date_column) %}
    to_char({{ date_column }}, 'YYYYMMDD')::int
{% endmacro %}

{% macro snowflake__date_to_key(date_column) %}
    to_number(to_char({{ date_column }}, 'YYYYMMDD'))
{% endmacro %}

--year
{% macro year_from_date(date_column) %}
    {{ return(adapter.dispatch('year_from_date')(date_column)) }}
{% endmacro %}

{% macro postgres__year_from_date(date_column) %}
    extract(year from {{ date_column }})
{% endmacro %}

{% macro snowflake__year_from_date(date_column) %}
    year({{ date_column }})
{% endmacro %}

--month
{% macro month_from_date(date_column) %}
    {{ return(adapter.dispatch('month_from_date')(date_column)) }}
{% endmacro %}

{% macro postgres__month_from_date(date_column) %}
    extract(month from {{ date_column }})
{% endmacro %}

{% macro snowflake__month_from_date(date_column) %}
    month({{ date_column }})
{% endmacro %}

--day
{% macro day_from_date(date_column) %}
    {{ return(adapter.dispatch('day_from_date')(date_column)) }}
{% endmacro %}

{% macro postgres__day_from_date(date_column) %}
    extract(day from {{ date_column }})
{% endmacro %}

{% macro snowflake__day_from_date(date_column) %}
    day({{ date_column }})
{% endmacro %}

--quarter
{% macro quarter_from_date(date_column) %}
    {{ return(adapter.dispatch('quarter_from_date')(date_column)) }}
{% endmacro %}

{% macro postgres__quarter_from_date(date_column) %}
    extract(quarter from {{ date_column }})
{% endmacro %}

{% macro snowflake__quarter_from_date(date_column) %}
    quarter({{ date_column }})
{% endmacro %}

--day of week
{% macro day_of_week(date_column) %}
    {{ return(adapter.dispatch('day_of_week')(date_column)) }}
{% endmacro %}

{% macro postgres__day_of_week(date_column) %}
    extract(isodow from {{ date_column }})
{% endmacro %}

{% macro snowflake__day_of_week(date_column) %}
    dayofweekiso({{ date_column }})
{% endmacro %}

--season
{% macro season_from_month(month_column) %}
    case
        when {{ month_column }} in (12, 1, 2) then 'Winter'
        when {{ month_column }} in (3, 4, 5) then 'Spring'
        when {{ month_column }} in (6, 7, 8) then 'Summer'
        else 'Autumn'
    end
{% endmacro %}

--weekend flag
{% macro is_weekend(date_column) %}
    case
        when {{ adapter.dispatch('day_of_week')(date_column) }} in (6, 7) then true
        else false
    end
{% endmacro %}


--time rounding
{% macro round_hour_from_time(time_column) %}
    {{ return(adapter.dispatch('round_hour_from_time')(time_column)) }}
{% endmacro %}

{% macro postgres__round_hour_from_time(time_column) %}

    case
        when extract(minute from {{ time_column }}) >= 30
            then extract(hour from {{ time_column }}) + 1
        else extract(hour from {{ time_column }})
    end

{% endmacro %}

{% macro snowflake__round_hour_from_time(time_column) %}

    case
        when date_part(minute, {{ time_column }}) >= 30
            then date_part(hour, {{ time_column }}) + 1
        else date_part(hour, {{ time_column }})
    end

{% endmacro %}