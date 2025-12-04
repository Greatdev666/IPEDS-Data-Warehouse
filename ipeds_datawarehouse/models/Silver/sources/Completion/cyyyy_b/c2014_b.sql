WITH c2014_b AS ( 
    SELECT * FROM {{ source('bronze', 'c2014_b') }}
)
, c2014_b_renamed AS (
    SELECT
        CAST(2014 AS INTEGER) AS "YEAR",
        CAST("UNITID" AS INTEGER) AS "UNITID",

        -- Grand Total 
        CAST("CSTOTLT" AS INTEGER) AS "CSTOTLT", 
        CAST("CSTOTLM" AS INTEGER) AS "CSTOTLM", 
        CAST("CSTOTLW" AS INTEGER) AS "CSTOTLW", 

        -- American Indian or Alaska Native
        CAST("CSAIANT" AS INTEGER) AS "CSAIANT", 
        CAST("CSAIANM" AS INTEGER) AS "CSAIANM", 
        CAST("CSAIANW" AS INTEGER) AS "CSAIANW", 

        -- Asian
        CAST("CSASIAT" AS INTEGER) AS "CSASIAT",   
        CAST("CSASIAM" AS INTEGER) AS "CSASIAM", 
        CAST("CSASIAW" AS INTEGER) AS "CSASIAW", 

        -- Black or African American
        CAST("CSBKAAT" AS INTEGER) AS "CSBKAAT", 
        CAST("CSBKAAM" AS INTEGER) AS "CSBKAAM", 
        CAST("CSBKAAW" AS INTEGER) AS "CSBKAAW", 

        -- Hispanic/Latino
        CAST("CSHISPT" AS INTEGER) AS "CSHISPT", 
        CAST("CSHISPM" AS INTEGER) AS "CSHISPM", 
        CAST("CSHISPW" AS INTEGER) AS "CSHISPW", 

        -- Native Hawaiian or Other Pacific Islander
        CAST("CSNHPIT" AS INTEGER) AS "CSNHPIT", 
        CAST("CSNHPIM" AS INTEGER) AS "CSNHPIM", 
        CAST("CSNHPIW" AS INTEGER) AS "CSNHPIW", 

        -- White
        CAST("CSWHITT" AS INTEGER) AS "CSWHITT", 
        CAST("CSWHITM" AS INTEGER) AS "CSWHITM", 
        CAST("CSWHITW" AS INTEGER) AS "CSWHITW", 

        -- Two or more races
        CAST("CS2MORT" AS INTEGER) AS "CS2MORT", 
        CAST("CS2MORM" AS INTEGER) AS "CS2MORM", 
        CAST("CS2MORW" AS INTEGER) AS "CS2MORW", 

        -- Race/ethnicity unknown
        CAST("CSUNKNT" AS INTEGER) AS "CSUNKNT", 
        CAST("CSUNKNM" AS INTEGER) AS "CSUNKNM", 
        CAST("CSUNKNW" AS INTEGER) AS "CSUNKNW", 

        -- Graduate Gender Unknown
        CAST(NULL AS INTEGER) AS "CSGUGUN", 

        -- Gradute Another Gender
        CAST(NULL AS INTEGER) AS "CSGUTOTAG",

        -- Non Resident Alien
        CAST("CSNRALT" AS INTEGER) AS "CSNRALT",
        CAST("CSNRALM" AS INTEGER) AS "CSNRALM", 
        CAST("CSNRALW" AS INTEGER) AS "CSNRALW"
    FROM c2014_b
)
SELECT * FROM c2014_b_renamed