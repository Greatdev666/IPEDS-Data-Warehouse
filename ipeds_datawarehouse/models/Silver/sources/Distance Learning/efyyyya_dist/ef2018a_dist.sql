WITH ef2018a_dist AS (
    SELECT * FROM {{ source('bronze', 'ef2018_dist') }}
)
, ef2018a_dist_renamed AS (
    SELECT
        CAST(2018 AS INTEGER) AS "YEAR",
        CAST("UNITID" AS INTEGER) AS "UNITID",

        -- Level of Student 
        CAST("EFDELEV" AS INTEGER) AS "EFDELEV",

        -- All students Enrolled
        CAST("EFDETOT" AS INTEGER) AS "EFDETOT",

        -- Students Exclusively Distance Education
        CAST("EFDEEXC" AS INTEGER) AS "EFDEEXC",

        -- Students Not Exclusively Distance Education just some courses
        CAST("EFDESOM" AS INTEGER) AS "EFDESOM",

        -- Student not enrolled in any distance education courses
        CAST("EFDENON" AS INTEGER) AS "EFDENON",

        -- Students enrolled exclusively in distance education courses and are located in same state/jurisdiction as institution
        CAST("EFDEEX1" AS INTEGER) AS "EFDEEX1",

        -- Students enrolled exclusively in distance education courses and are located in U.S., not in same state/jurisdiction as institution
        CAST("EFDEEX2" AS INTEGER) AS "EFDEEX2",

        -- Students enrolled exclusively in distance education courses and are located in U.S., state/jurisdiction unknown
        CAST("EFDEEX3" AS INTEGER) AS "EFDEEX3",

        -- Students enrolled exclusively in distance education courses and are located outside U.S.
        CAST("EFDEEX4" AS INTEGER) AS "EFDEEX4",

        -- Students enrolled exclusively in distance education courses and location of student unknown/not reported
        CAST("EFDEEX5" AS INTEGER) AS "EFDEEX5"

    FROM ef2018a_dist
)

SELECT * FROM ef2018a_dist_renamed