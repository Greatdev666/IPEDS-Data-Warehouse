WITH base AS (
    SELECT 
        "YEAR" AS year,
        "UNITID" AS unit_id,
        "EFFYDLEV" AS student_level,
        SUM("EFYDETOT") AS total_enrolled,
        SUM("EFYDEEXC") AS fully_online,
        SUM("EFYDESOM") AS hybrid,
        SUM("EFYDENON") AS in_person
    FROM {{ ref('effyyyyy_dist') }}
    GROUP BY 1,2,3
),
unpivoted AS (
    SELECT
        year,
        unit_id,
        student_level,
        learning_mode_key,
        total_enrolled,
        learning_mode
    FROM base 
    CROSS JOIN LATERAL (
        VALUES 
            (1, fully_online),
            (2, hybrid),
            (3, in_person)
    ) AS l (learning_mode_key, learning_mode)
)

SELECT 
    year,
    unit_id,
    student_level,
    total_enrolled,
    learning_mode_key,
    COALESCE(learning_mode, 0) AS learning_mode
FROM unpivoted