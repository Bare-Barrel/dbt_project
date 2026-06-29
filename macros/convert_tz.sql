-- convert_tz.sql

{% macro marketplace_tz(column, marketplace_col) %}
    datetime(
        {{ column }},
        case {{ marketplace_col }}
            when 'US' then 'America/Los_Angeles'
            when 'CA' then 'America/Los_Angeles'
            when 'UK' then 'Europe/London'
        end
    )
{% endmacro %}