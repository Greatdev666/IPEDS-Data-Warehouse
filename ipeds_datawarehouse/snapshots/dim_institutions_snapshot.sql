{% snapshot dim_institutions_snapshot %}

{{
    config(
        target_schema='Silver_Gold',
        target_database='IPEDSDataWarehouse',
        strategy='check',
        unique_key='unit_id',
        check_cols=[
            'instnm',
            'ialias',
            'city',
            'stabbr',
            'zip',
            'obereg',
            'longitud',
            'latitude',
            'sector',
            'iclevel',
            'control',
            'hloffer',
            'ugoffer',
            'groffer',
            'hdegofr1',
            'instsize',
            'webaddr',
            'adminurl',
            'faidurl'
        ]
    )
}}

SELECT
    "UNITID" AS unit_id,
    "INSTNM" AS instnm,
    "IALIAS" AS ialias,
    "CITY" AS city,
    "STABBR" AS stabbr,
    "ZIP" AS zip,
    "OBEREG" AS obereg,
    "LONGITUD" AS longitud,
    "LATITUDE" AS latitude,
    "SECTOR" AS sector,
    "ICLEVEL" AS iclevel,
    "CONTROL" AS control,
    "HLOFFER" AS hloffer,
    "UGOFFER" AS ugoffer,
    "GROFFER" AS groffer,
    "HDEGOFR1" AS hdegofr1,
    "INSTSIZE" AS instsize,
    {{ clean_links('"WEBADDR"') }} AS webaddr,
    {{ clean_links('"ADMINURL"') }} AS adminurl,
    {{ clean_links('"FAIDURL"') }} AS faidurl
FROM {{ ref('hdyyyy') }}

{% endsnapshot %}
