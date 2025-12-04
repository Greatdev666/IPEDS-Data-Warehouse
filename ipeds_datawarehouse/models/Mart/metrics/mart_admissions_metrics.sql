WITH base AS (
    SELECT * FROM {{ ref('mart_admissions') }}
),
agg AS (
    SELECT 
        academic_year_id,
        unit_id,
        institution_name,
        city,
        state_abbreviation,
        control_type,
        sector,
        institution_level,
        total_applicants,
        total_admissions, 
        total_enrolled
    FROM base 
),
metrics AS (
    SELECT 
        a.*,
        -- Admission rate 
        CASE 
        WHEN a.total_applicants = 0 THEN NULL ELSE 
        (a.total_admissions::decimal / a.total_applicants) 
        END AS admission_rate ,

        -- Yield Rate
        CASE 
        WHEN a.total_admissions = 0 THEN NULL ELSE 
        (a.total_enrolled::decimal / a.total_admissions) 
        END AS yield_rate,

        --YoY absolute change    
        a.total_applicants - LAG(total_applicants) OVER(
            PARTITION BY a.unit_id ORDER BY a.academic_year_id
        ) applicants_yoy_change,
        a.total_admissions - LAG(total_admissions) OVER(
            PARTITION BY a.unit_id ORDER BY a.academic_year_id 
        ) admission_yoy_change,

        a.total_enrolled - LAG(total_enrolled) OVER(
            PARTITION BY a.unit_id ORDER BY a.academic_year_id 
        ) enrollment_yoy_change,

        -- YoY percent change
        CASE 
            WHEN LAG(total_applicants) OVER(
                PARTITION BY a.unit_id ORDER BY a.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (
                    a.total_applicants::decimal 
                      / LAG(a.total_applicants) OVER(
                        PARTITION BY a.unit_id ORDER BY a.academic_year_id
                      ) 
                ) - 1
        END applicants_yoy_percent,

        CASE 
            WHEN LAG(total_admissions) OVER(
                PARTITION BY a.unit_id ORDER BY a.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (
                    a.total_admissions::decimal 
                      / LAG(a.total_admissions) OVER(
                        PARTITION BY a.unit_id ORDER BY a.academic_year_id
                      ) 
                ) - 1
        END admissions_yoy_percent,
        CASE 
            WHEN LAG(total_enrolled) OVER(
                PARTITION BY a.unit_id ORDER BY a.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (
                    a.total_enrolled::decimal 
                      / LAG(a.total_enrolled) OVER(
                        PARTITION BY a.unit_id ORDER BY a.academic_year_id
                      ) 
                ) - 1
        END enrollment_yoy_percent

    FROM agg a 
)

SELECT * FROM metrics