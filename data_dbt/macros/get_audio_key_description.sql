{% macro get_audio_key_description(key) %}

    case {{ dbt.safe_cast("key", api.Column.translate_type("integer")) }}
        when 0 then 'C'
        when 1 then 'C♯/D♭'
        when 2 then 'D'
        when 3 then 'D♯/E♭'
        when 4 then 'E'
        when 5 then 'F'
        when 6 then 'F♯/G♭'
        when 7 then 'G'
        when 8 then 'G♯/A♭'
        when 9 then 'A'
        when 10 then 'A♯, B♭'
        when 11 then 'B'
        else 'no key'
    end

{% endmacro %}
