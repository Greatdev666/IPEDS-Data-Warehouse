{{ config(materialized='table') }}

WITH award_levels AS (
    SELECT *
    FROM ( 
        values 
            (1,  'Less-than-1-year certificate'),
            (2,  '1- to 2-year certificate'),
            (3,  'Associate’s degree'),
            (4,  '2- to 4-year certificate'),
            (5,  'Bachelor’s degree'),
            (6,  'Postbaccalaureate certificate'),
            (7,  'Master’s degree'),
            (8,  'Post-master’s certificate'),
            (17, 'Doctor’s degree – research/scholarship'),
            (18, 'Doctor’s degree – professional practice'),
            (19, 'Doctor’s degree – other'),
            (20, 'Other award level (IPEDS special category)'),
            (21, 'Reserved for institutional reporting')
    ) AS t(award_level_code, award_level_name)
)

SELECT
    award_level_code,
    award_level_name
from award_levels
