INSERT INTO olympics.athletes
SELECT
    CAST(athlete_id AS INTEGER),
    name,
    sex,
    CASE WHEN height = '' OR height IS NULL THEN NULL
         ELSE CAST(height AS NUMERIC(5,1))
    END,
    CASE WHEN weight = '' OR weight IS NULL THEN NULL
         ELSE CAST(weight AS NUMERIC(5,1))
    END
FROM olympics.stg_athletes;

INSERT INTO olympics.results
SELECT
    CAST(result_id AS BIGINT),
    CAST(athlete_id AS INTEGER),
    CAST(game_id AS INTEGER),
    CAST(event_id AS INTEGER),
    noc,
    team,
    CASE WHEN age = '' OR age IS NULL OR age = '0' THEN NULL
         ELSE CAST(ROUND(CAST(age AS NUMERIC)) AS INTEGER)
    END,
    COALESCE(NULLIF(medal, ''), 'None')
FROM olympics.stg_results;

SELECT 'athletes' AS tabel, COUNT(*) AS nr FROM olympics.athletes
UNION ALL
SELECT 'results', COUNT(*) FROM olympics.results;
