{% macro stress_band(score_col) %}
    CASE
        WHEN {{ score_col }} >= 0.70 THEN 'CRITICAL'
        WHEN {{ score_col }} >= 0.50 THEN 'HIGH'
        WHEN {{ score_col }} >= 0.30 THEN 'MODERATE'
        ELSE 'LOW'
    END
{% endmacro %}

{% macro composite_stress_score(sp, ii, hre, is_, pd, ad, imp) %}
    ROUND({{ sp }}*0.25 + {{ ii }}*0.20 + {{ hre }}*0.15
        + {{ is_ }}*0.20 + {{ pd }}*0.10 + {{ ad }}*0.05 + {{ imp }}*0.05, 4)
{% endmacro %}

{% macro safe_ratio(num, den, decimals=4) %}
    ROUND({{ num }} / NULLIF({{ den }}, 0), {{ decimals }})
{% endmacro %}
