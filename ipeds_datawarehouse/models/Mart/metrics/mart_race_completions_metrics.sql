WITH base AS (
    SELECT * FROM {{ ref('mart_race_completions') }}
), 
yoy AS (
    SELECT
        -- YoY absolute change 
        b.*,

        b.total_completions - 
            LAG(b.total_completions) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS total_completions_yoy_change,

        -- Race breakdown yoy change
            
        b.white_completions - 
            LAG(b.white_completions) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS white_completions_yoy_change,
            
        b.black_or_african_completions - 
            LAG(b.black_or_african_completions) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS black_or_african_completions_yoy_change,
            
        b.hispanic_or_latino_completions - 
            LAG(b.hispanic_or_latino_completions) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS hispanic_or_latino_completions_yoy_change,
            
        b.asian_completions - 
            LAG(b.asian_completions) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS asian_completions_yoy_change,
            
        b.american_indian_or_alaska_native_completions - 
            LAG(b.american_indian_or_alaska_native_completions) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS american_indian_or_alaska_native_completions_yoy_change,
            
        b.pacific_islander_or_native_hawaiian_completions - 
            LAG(b.pacific_islander_or_native_hawaiian_completions) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS pacific_islander_or_native_hawaiian_completions_yoy_change,
            
        b.two_or_more_races_completions - 
            LAG(b.two_or_more_races_completions) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS two_or_more_races_completions_yoy_change,
            
        b.nonresident_alien_completions - 
            LAG(b.nonresident_alien_completions) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS nonresident_alien_completions_yoy_change,

        b.unknown_race_completions - 
            LAG(b.unknown_race_completions) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS unknown_race_completions_yoy_change,

        -- YoY percent change
        CASE 
            WHEN LAG(b.total_completions) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.total_completions::decimal
                   / LAG(b.total_completions) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END completions_yoy_percent,
        
        --Race break down yoy % change

        CASE 
            WHEN LAG(b.white_completions) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.white_completions::decimal
                   / LAG(b.white_completions) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END white_completions_yoy_percent,

        CASE 
            WHEN LAG(b.black_or_african_completions) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.black_or_african_completions::decimal
                   / LAG(b.black_or_african_completions) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END black_or_african_completions_yoy_percent,

        CASE 
            WHEN LAG(b.hispanic_or_latino_completions) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.hispanic_or_latino_completions::decimal
                   / LAG(b.hispanic_or_latino_completions) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END hispanic_or_latino_completions_yoy_percent,

        CASE 
            WHEN LAG(b.asian_completions) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.asian_completions::decimal
                   / LAG(b.asian_completions) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END asian_completions_yoy_percent,

        CASE 
            WHEN LAG(b.american_indian_or_alaska_native_completions) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.american_indian_or_alaska_native_completions::decimal
                   / LAG(b.american_indian_or_alaska_native_completions) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END american_indian_or_alaska_native_completions_yoy_percent,

        CASE 
            WHEN LAG(b.pacific_islander_or_native_hawaiian_completions) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.pacific_islander_or_native_hawaiian_completions::decimal
                   / LAG(b.pacific_islander_or_native_hawaiian_completions) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END pacific_islander_or_native_hawaiian_completions_yoy_percent,

        CASE 
            WHEN LAG(b.two_or_more_races_completions) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.two_or_more_races_completions::decimal
                   / LAG(b.two_or_more_races_completions) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END two_or_more_races_completions_yoy_percent,

        CASE 
            WHEN LAG(b.nonresident_alien_completions) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.nonresident_alien_completions::decimal
                   / LAG(b.nonresident_alien_completions) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END nonresident_alien_completions_yoy_percent,

        CASE 
            WHEN LAG(b.unknown_race_completions) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.unknown_race_completions::decimal
                   / LAG(b.unknown_race_completions) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END unknown_race_completions_yoy_percent
    FROM base b
),
race_percent_share_of_institution AS (
    SELECT 
        y.*,
        
        -- Race Share of Institution
        CASE WHEN y.total_completions = 0 THEN NULL
        ELSE y.white_completions::decimal / y.total_completions END AS white_share,

        CASE WHEN y.total_completions = 0 THEN NULL
        ELSE y.black_or_african_completions::decimal / y.total_completions END AS black_share,

        CASE WHEN y.total_completions = 0 THEN NULL
        ELSE y.hispanic_or_latino_completions::decimal / y.total_completions END AS hispanic_share,

        CASE WHEN y.total_completions = 0 THEN NULL
        ELSE y.asian_completions::decimal / y.total_completions END AS asian_share,

        CASE WHEN y.total_completions = 0 THEN NULL
        ELSE y.american_indian_or_alaska_native_completions::decimal / y.total_completions END AS native_share,

        CASE WHEN y.total_completions = 0 THEN NULL
        ELSE y.pacific_islander_or_native_hawaiian_completions::decimal / y.total_completions END AS pacific_share,

        CASE WHEN y.total_completions = 0 THEN NULL
        ELSE y.two_or_more_races_completions::decimal / y.total_completions END AS two_or_more_share,

        CASE WHEN y.total_completions = 0 THEN NULL
        ELSE y.nonresident_alien_completions::decimal / y.total_completions END AS nonresident_share,

        CASE WHEN y.total_completions = 0 THEN NULL
        ELSE y.unknown_race_completions::decimal / y.total_completions END AS unknown_share

    FROM yoy y 
),
race_underrepresentation_indicators AS (
    SELECT 
    r.*,
    (r.white_share - e.white_enrollment) AS white_underrepresentation,
    (r.black_share - e.black_share) AS black_underrepresentation,
    r.hispanic_share - e.hispanic_share AS hispanic_or_latino_underrepresentation,
    r.asian_share - e.asian_share AS asian_underrepresentation,
    r.native_share - e.native_share AS american_indian_or_alaska_native_underrepresentation,
    r.pacific_share - e.pacific_share AS pacific_islander_or_native_hawaiian_underrepresentation,
    r.two_or_more_share - e.two_or_more_share AS two_or_more_races_underrepresentation,
    r.nonresident_share - e.nonresident_share AS nonresident_alien_underrepresentation,
    r.unknown_share - e.unknown_share AS unknown_race_underrepresentation
    FROM race_percent_share_of_institution r 
    LEFT JOIN {{ ref('mart_race_enrollment_metrics') }} e
    ON (r.unit_id = e.unit_id)
    AND r.academic_year_id = e.academic_year_id 
)

SELECT * FROM race_underrepresentation_indicators