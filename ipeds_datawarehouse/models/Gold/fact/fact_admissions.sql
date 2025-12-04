WITH 
adm AS (
    SELECT 
        "UNITID" as unit_id,
        "YEAR" as year,
        SUM("APPLCN") as total_applicants,
        SUM("ADMSSN") as total_admissions,
        SUM("ENRLT") as total_enrolled
     FROM {{ ref('adm') }}
     GROUP BY 1,2
), 
acy AS (
    SELECT * FROM {{ ref('dim_academic_year') }}
)

SELECT 
    adm.unit_id,
    acy.academic_year_id,
    COALESCE(adm.total_applicants, 0) AS total_applicants,
    COALESCE(adm.total_admissions, 0) AS total_admissions,
    COALESCE(adm.total_enrolled, 0) AS total_enrolled
FROM adm
LEFT JOIN acy on (acy.academic_year_id = adm.year)