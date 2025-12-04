WITH base AS (
    SELECT * FROM {{ ref('mart_race_enrollment') }}
), 
yoy AS (
    SELECT
        -- YoY absolute change 
        b.*,

        b.total_enrollment - 
            LAG(b.total_enrollment) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS total_enrollment_yoy_change,

        -- Race breakdown yoy change
            
        b.white_enrollment - 
            LAG(b.white_enrollment) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS white_enrollment_yoy_change,
            
        b.black_or_african_enrollment - 
            LAG(b.black_or_african_enrollment) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS black_or_african_enrollment_yoy_change,
            
        b.hispanic_or_latino_enrollment - 
            LAG(b.hispanic_or_latino_enrollment) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS hispanic_or_latino_enrollment_yoy_change,
            
        b.asian_enrollment - 
            LAG(b.asian_enrollment) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS asian_enrollment_yoy_change,
            
        b.american_indian_or_alaska_native_enrollment - 
            LAG(b.american_indian_or_alaska_native_enrollment) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS american_indian_or_alaska_native_enrollment_yoy_change,
            
        b.pacific_islander_or_native_hawaiian_enrollment - 
            LAG(b.pacific_islander_or_native_hawaiian_enrollment) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS pacific_islander_or_native_hawaiian_enrollment_yoy_change,
            
        b.two_or_more_races_enrollment - 
            LAG(b.two_or_more_races_enrollment) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS two_or_more_races_enrollment_yoy_change,
            
        b.nonresident_alien_enrollment - 
            LAG(b.nonresident_alien_enrollment) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS nonresident_alien_enrollment_yoy_change,

        b.unknown_race_enrollment - 
            LAG(b.unknown_race_enrollment) OVER(PARTITION BY b.unit_id ORDER BY b.academic_year_id)
            AS unknown_race_enrollment_yoy_change,

        -- YoY percent change
        CASE 
            WHEN LAG(b.total_enrollment) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.total_enrollment::decimal
                   / LAG(b.total_enrollment) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END enrollment_yoy_percent,
        
        --Race break down yoy % change

        CASE 
            WHEN LAG(b.white_enrollment) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.white_enrollment::decimal
                   / LAG(b.white_enrollment) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END white_enrollment_yoy_percent,

        CASE 
            WHEN LAG(b.black_or_african_enrollment) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.black_or_african_enrollment::decimal
                   / LAG(b.black_or_african_enrollment) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END black_or_african_enrollment_yoy_percent,

        CASE 
            WHEN LAG(b.hispanic_or_latino_enrollment) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.hispanic_or_latino_enrollment::decimal
                   / LAG(b.hispanic_or_latino_enrollment) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END hispanic_or_latino_enrollment_yoy_percent,

        CASE 
            WHEN LAG(b.asian_enrollment) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.asian_enrollment::decimal
                   / LAG(b.asian_enrollment) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END asian_enrollment_yoy_percent,

        CASE 
            WHEN LAG(b.american_indian_or_alaska_native_enrollment) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.american_indian_or_alaska_native_enrollment::decimal
                   / LAG(b.american_indian_or_alaska_native_enrollment) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END american_indian_or_alaska_native_enrollment_yoy_percent,

        CASE 
            WHEN LAG(b.pacific_islander_or_native_hawaiian_enrollment) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.pacific_islander_or_native_hawaiian_enrollment::decimal
                   / LAG(b.pacific_islander_or_native_hawaiian_enrollment) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END pacific_islander_or_native_hawaiian_enrollment_yoy_percent,

        CASE 
            WHEN LAG(b.two_or_more_races_enrollment) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.two_or_more_races_enrollment::decimal
                   / LAG(b.two_or_more_races_enrollment) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END two_or_more_races_enrollment_yoy_percent,

        CASE 
            WHEN LAG(b.nonresident_alien_enrollment) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.nonresident_alien_enrollment::decimal
                   / LAG(b.nonresident_alien_enrollment) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END nonresident_alien_enrollment_yoy_percent,

        CASE 
            WHEN LAG(b.unknown_race_enrollment) OVER(
                PARTITION BY b.unit_id ORDER BY b.academic_year_id
            ) = 0
            THEN NULL
            ELSE 
                (b.unknown_race_enrollment::decimal
                   / LAG(b.unknown_race_enrollment) OVER(
                    PARTITION BY b.unit_id ORDER BY b.academic_year_id
                    )
                ) - 1 
        END unknown_race_enrollment_yoy_percent
    FROM base b
),
race_percent_share_of_institution AS (
    SELECT 
        y.*,
        
        -- Race Share of Institution
        CASE WHEN y.total_enrollment = 0 THEN NULL
        ELSE y.white_enrollment::decimal / y.total_enrollment END AS white_share,

        CASE WHEN y.total_enrollment = 0 THEN NULL
        ELSE y.black_or_african_enrollment::decimal / y.total_enrollment END AS black_share,

        CASE WHEN y.total_enrollment = 0 THEN NULL
        ELSE y.hispanic_or_latino_enrollment::decimal / y.total_enrollment END AS hispanic_share,

        CASE WHEN y.total_enrollment = 0 THEN NULL
        ELSE y.asian_enrollment::decimal / y.total_enrollment END AS asian_share,

        CASE WHEN y.total_enrollment = 0 THEN NULL
        ELSE y.american_indian_or_alaska_native_enrollment::decimal / y.total_enrollment END AS native_share,

        CASE WHEN y.total_enrollment = 0 THEN NULL
        ELSE y.pacific_islander_or_native_hawaiian_enrollment::decimal / y.total_enrollment END AS pacific_share,

        CASE WHEN y.total_enrollment = 0 THEN NULL
        ELSE y.two_or_more_races_enrollment::decimal / y.total_enrollment END AS two_or_more_share,

        CASE WHEN y.total_enrollment = 0 THEN NULL
        ELSE y.nonresident_alien_enrollment::decimal / y.total_enrollment END AS nonresident_share,

        CASE WHEN y.total_enrollment = 0 THEN NULL
        ELSE y.unknown_race_enrollment::decimal / y.total_enrollment END AS unknown_share

    FROM yoy y 
),
race_gap_metrics AS (
    SELECT 
    r.*,
    -- Race gap metrics
    -- Race Gaps
    (black_share - white_share) AS black_white_gap,
    (hispanic_share - white_share) AS hispanic_white_gap,
    (asian_share - white_share) AS asian_white_gap,
    (native_share - white_share) AS native_white_gap,
    (pacific_share - white_share) AS pacific_white_gap,
    (two_or_more_share - white_share) AS two_or_more_white_gap

    FROM race_percent_share_of_institution r
),
underrepresented_minority AS (
    SELECT 
    r.*,
    -- URM Share
    /* URM Share =   Black + Hispanic + Native American + Pacific Islander + Two or More 
                     / Total Enrollment
    */

    (
        COALESCE(black_or_african_enrollment,0) 
        + COALESCE(hispanic_or_latino_enrollment,0) 
        + COALESCE(american_indian_or_alaska_native_enrollment,0) 
        + COALESCE(pacific_islander_or_native_hawaiian_enrollment,0) 
        + COALESCE(two_or_more_races_enrollment,0)
    )::decimal / NULLIF(total_enrollment, 0) AS urm_share

    FROM race_gap_metrics r
),
diversity_index AS (
    SELECT 
    urm.*,
    1
    - (
        (white_share::decimal / NULLIF(total_enrollment, 0))^2
        + (black_share::decimal / NULLIF(total_enrollment, 0))^2
        + (hispanic_share::decimal / NULLIF(total_enrollment, 0))^2
        + (asian_share::decimal / NULLIF(total_enrollment, 0))^2
        + (native_share::decimal / NULLIF(total_enrollment, 0))^2
        + (pacific_share::decimal / NULLIF(total_enrollment, 0))^2
        + (two_or_more_share::decimal / NULLIF(total_enrollment, 0))^2
        + (nonresident_share::decimal / NULLIF(total_enrollment, 0))^2
        + (unknown_share::decimal / NULLIF(total_enrollment, 0))^2
    ) AS diversity_index

    FROM underrepresented_minority urm 
)

SELECT * FROM diversity_index