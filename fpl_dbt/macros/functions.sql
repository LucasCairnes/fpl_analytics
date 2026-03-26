{% macro rolling_value(column, func, weeks) %}

    {% if func | upper == 'AVG' %}
        ROUND(AVG({{ column }}) OVER(
            PARTITION BY player_id
            ORDER BY gameweek 
            RANGE BETWEEN {{ weeks - 1 }} PRECEDING AND CURRENT ROW
        ), 3)
    {% else %}
        SUM({{ column }}) OVER(
            PARTITION BY player_id
            ORDER BY gameweek 
            RANGE BETWEEN {{ weeks - 1 }} PRECEDING AND CURRENT ROW
        )
    {% endif %}

{% endmacro %}