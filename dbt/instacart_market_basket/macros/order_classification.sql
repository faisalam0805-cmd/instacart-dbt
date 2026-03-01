{% macro classify_order(eval_set_column) %}
    case
        when {{ eval_set_column }} = 'prior' then 'historical'
        when {{ eval_set_column }} = 'train' then 'training'
        when {{ eval_set_column }} = 'test' then 'prediction'
        else 'unknown'
    end
{% endmacro %}