WITH base AS (
    SELECT * FROM {{ ref('mart_enrollment') }}
), 
agg AS (
    SELECT 
        unit_id,
        academic_year_id,
        institution_name,
        citY,
        state_abbreviation,
        control_type,
        sector,
        institution_level,

        -- Total Enrollment
        SUM(total_enrolled) as total_enrollment,

        -- Level-based enrollment
        SUM(CASE WHEN enrollment_level = 2 THEN total_enrolled END) AS total_undergraduates_enrollment,
        SUM(CASE WHEN enrollment_level = 4 THEN total_enrolled END) AS total_graduates_enrollment,

        --Gender Split
        SUM(total_male_enrolled) AS total_male_enrollment,
        SUM(total_female_enrolled) AS total_female_enrollment
    FROM base
    GROUP BY 1,2,3,4,5,6,7,8
),
yoy AS (
    SELECT
        -- YoY absolute change 
        a.*,
        a.total_enrollment - 
            LAG(a.total_enrollment) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id)
            AS enrollment_yoy_change,
        -- YoY percent change
        CASE 
            WHEN LAG(a.total_enrollment) OVER(
                PARTITION BY a.unit_id ORDER BY a.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (a.total_enrollment::decimal
                   / LAG(a.total_enrollment) OVER(
                    PARTITION BY a.unit_id ORDER BY a.academic_year_id
                    )
                ) - 1 
        END enrollment_yoy_percent,

        --Gender %
        CASE 
            WHEN a.total_enrollment = 0 THEN NULL 
            ELSE a.total_male_enrollment::decimal / a.total_enrollment
        END male_enrollment_percent,

        CASE 
            WHEN a.total_enrollment = 0 THEN NULL 
            ELSE a.total_female_enrollment::decimal / a.total_enrollment
        END female_enrollment_percent,


        --Gender YoY absolute change

        a.total_male_enrollment - 
        LAG(a.total_male_enrollment) OVER(
            PARTITION BY a.unit_id ORDER BY a.academic_year_id
        )  AS male_enrolled_yoy_change,

        a.total_female_enrollment - 
        LAG(a.total_female_enrollment) OVER(
            PARTITION BY a.unit_id ORDER BY a.academic_year_id
        ) AS female_enrolled_yoy_change,

        -- Gender YoY %
        CASE 
            WHEN LAG(a.total_male_enrollment) OVER(
                PARTITION BY a.unit_id ORDER BY a.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
            (a.total_male_enrollment::decimal
               / LAG(a.total_male_enrollment) OVER(
                PARTITION BY a.unit_id ORDER BY a.academic_year_id
                ) 
            )- 1
        END male_enrolled_yoy_percent,

         CASE 
            WHEN LAG(a.total_female_enrollment) OVER(
                PARTITION BY a.unit_id ORDER BY a.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
            (a.total_female_enrollment::decimal
               / LAG(a.total_female_enrollment) OVER(
                PARTITION BY a.unit_id ORDER BY a.academic_year_id
                )
            ) - 1
        END female_enrolled_yoy_percent
           
    FROM agg a
)

SELECT * FROM yoy