WITH base AS (
    SELECT * FROM {{ ref('mart_outcome') }}
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

        -- Total Graduations
        SUM(total_graduates) as total_graduates,

        --Gender Split
        SUM(total_male_graduates) AS total_male_graduates,
        SUM(total_female_graduates) AS total_female_graduates,

         -- Cohort size (denominator)
        SUM(CASE WHEN graduation_type = 54 THEN total_graduates END) AS cohort_size,
        -- Exclusions
        SUM(CASE WHEN graduation_type = 320 THEN total_graduates END) AS exclusions,
        -- Completers at 100%
        SUM(CASE WHEN graduation_type = 51 THEN total_graduates END) AS completions_100,
        -- Completers at 150%
        SUM(CASE WHEN graduation_type = 52 THEN total_graduates END) AS completions_150,
        -- Transfer out
        SUM(CASE WHEN graduation_type = 319 THEN total_graduates END) AS transfer_out

    FROM base
    GROUP BY 1,2,3,4,5,6,7,8
),
yoy AS (
    SELECT
        -- YoY absolute change 
        a.*,
        a.total_graduates - 
            LAG(a.total_graduates) OVER(PARTITION BY a.unit_id ORDER BY a.academic_year_id)
            AS graduation_yoy_change,
        -- YoY percent change
        CASE 
            WHEN LAG(a.total_graduates) OVER(
                PARTITION BY a.unit_id ORDER BY a.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (a.total_graduates::decimal
                   / LAG(a.total_graduates) OVER(
                    PARTITION BY a.unit_id ORDER BY a.academic_year_id
                    )
                ) - 1 
        END graduation_yoy_percent,

        --Gender %
        CASE 
            WHEN a.total_graduates = 0 THEN NULL 
            ELSE a.total_male_graduates::decimal / a.total_graduates
        END male_graduation_percent,

        CASE 
            WHEN a.total_graduates = 0 THEN NULL 
            ELSE a.total_female_graduates::decimal / a.total_graduates
        END female_graduation_percent,


        --Gender YoY absolute change

        a.total_male_graduates - 
        LAG(a.total_male_graduates) OVER(
            PARTITION BY a.unit_id ORDER BY a.academic_year_id
        )  AS male_graduates_yoy_change,

        a.total_female_graduates - 
        LAG(a.total_female_graduates) OVER(
            PARTITION BY a.unit_id ORDER BY a.academic_year_id
        ) AS female_graduates_yoy_change,

        -- Gender YoY %
        CASE 
            WHEN LAG(a.total_male_graduates) OVER(
                PARTITION BY a.unit_id ORDER BY a.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
            (a.total_male_graduates::decimal
               / LAG(a.total_male_graduates) OVER(
                PARTITION BY a.unit_id ORDER BY a.academic_year_id
                ) 
            )- 1
        END male_graduates_yoy_percent,

         CASE 
            WHEN LAG(a.total_female_graduates) OVER(
                PARTITION BY a.unit_id ORDER BY a.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
            (a.total_female_graduates::decimal
               / LAG(a.total_female_graduates) OVER(
                PARTITION BY a.unit_id ORDER BY a.academic_year_id
                )
            ) - 1
        END female_graduates_yoy_percent,

         -- denominator after removing exclusions
        (a.cohort_size - exclusions) AS adjusted_cohort,

        (a.completions_150 / NULLIF((a.cohort_size - a.exclusions), 0)) AS graduation_rate_150,
        (a.completions_100 / NULLIF((a.cohort_size - a.exclusions), 0)) AS graduation_rate_100,
        (transfer_out     / NULLIF((a.cohort_size - a.exclusions), 0)) AS transfer_out_rate
           
    FROM agg a
)

SELECT * FROM yoy 