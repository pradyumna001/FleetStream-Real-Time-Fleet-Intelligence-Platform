{#
    Great-circle distance in kilometres between two lat/lon pairs.

    Used to derive trip distance from consecutive GPS fixes as a cross-check against
    the odometer delta. Two independent measures of the same quantity make a bad
    reading detectable; one measure just has to be trusted.
#}
{% macro haversine_km(lat1, lon1, lat2, lon2) %}
    (2 * 6371.0088 * asin(sqrt(
        power(sin(radians({{ lat2 }} - {{ lat1 }}) / 2), 2)
        + cos(radians({{ lat1 }})) * cos(radians({{ lat2 }}))
        * power(sin(radians({{ lon2 }} - {{ lon1 }}) / 2), 2)
    )))
{% endmacro %}
