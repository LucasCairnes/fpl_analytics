{% macro rolling_value(column, func, weeks) %}

    {% if func | upper == 'AVG' %}
        ROUND(AVG({{ column }}) OVER(
            PARTITION BY player_id
            ORDER BY gameweek DESC
            ROWS BETWEEN CURRENT ROW AND {{ weeks - 1 }} FOLLOWING
        ), 3)
    {% else %}
        SUM({{ column }}) OVER(
            PARTITION BY player_id
            ORDER BY gameweek DESC
            ROWS BETWEEN CURRENT ROW AND {{ weeks }} FOLLOWING
        )
    {% endif %}

{% endmacro %}




