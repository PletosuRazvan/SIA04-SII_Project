CREATE OR REPLACE VIEW DIM_ATHLETE_V AS
SELECT
    athlete_id,
    name,
    sex,
    height,
    weight,
    total_medals,
    gold,
    silver,
    bronze,
    sports_count,
    games_count,
    CASE
        WHEN total_medals = 0 THEN 'NO_MEDAL'
        WHEN total_medals <= 2 THEN 'FEW_MEDALS'
        WHEN total_medals <= 5 THEN 'MULTIPLE_MEDALS'
        ELSE 'ELITE_MEDALIST'
    END AS medal_category,
    CASE
        WHEN gold > 0 AND silver > 0 AND bronze > 0 THEN 'ALL_COLORS'
        WHEN gold > 0 THEN 'GOLD_WINNER'
        WHEN silver > 0 THEN 'SILVER_WINNER'
        WHEN bronze > 0 THEN 'BRONZE_WINNER'
        ELSE 'NO_MEDAL'
    END AS medal_profile,
    CASE
        WHEN height IS NULL THEN 'UNKNOWN'
        WHEN height < 165 THEN 'SHORT'
        WHEN height < 180 THEN 'MEDIUM'
        WHEN height < 195 THEN 'TALL'
        ELSE 'VERY_TALL'
    END AS height_group,
    CASE sex
        WHEN 'M' THEN 'MALE'
        WHEN 'F' THEN 'FEMALE'
        ELSE 'UNKNOWN'
    END AS gender_label
FROM INT_ATHLETE_PROFILE_V;

SELECT * FROM DIM_ATHLETE_V WHERE ROWNUM <= 10;

CREATE OR REPLACE VIEW DIM_GAME_V AS
SELECT
    game_id,
    games_name,
    year,
    season,
    city,
    FLOOR(year / 10) * 10 AS decade,
    CASE
        WHEN year < 1920 THEN 'PIONEER_ERA'
        WHEN year < 1950 THEN 'EARLY_ERA'
        WHEN year < 1980 THEN 'CLASSIC_ERA'
        WHEN year < 2000 THEN 'MODERN_ERA'
        ELSE 'CONTEMPORARY_ERA'
    END AS era,
    CASE season
        WHEN 'Summer' THEN 'SUMMER_GAMES'
        WHEN 'Winter' THEN 'WINTER_GAMES'
    END AS season_type
FROM GAMES_V;

SELECT * FROM DIM_GAME_V ORDER BY year;

CREATE OR REPLACE VIEW DIM_EVENT_V AS
SELECT
    event_id,
    event_name,
    sport_id,
    sport_name
FROM INT_EVENT_CATALOG_V;

SELECT * FROM DIM_EVENT_V WHERE ROWNUM <= 10;

CREATE OR REPLACE VIEW DIM_COUNTRY_V AS
SELECT
    noc,
    region AS country_name,
    notes
FROM COUNTRIES_V;

SELECT * FROM DIM_COUNTRY_V WHERE ROWNUM <= 10;

CREATE OR REPLACE VIEW FACT_RESULTS_V AS
SELECT
    result_id,
    athlete_id,
    game_id,
    event_id,
    noc,
    team,
    age,
    medal,
    CASE WHEN medal != 'None' THEN 1 ELSE 0 END AS is_medalist,
    CASE WHEN medal = 'Gold'   THEN 1 ELSE 0 END AS is_gold,
    CASE WHEN medal = 'Silver' THEN 1 ELSE 0 END AS is_silver,
    CASE WHEN medal = 'Bronze' THEN 1 ELSE 0 END AS is_bronze,
    CASE
        WHEN age IS NULL THEN 'UNKNOWN'
        WHEN age < 20 THEN 'JUNIOR'
        WHEN age < 25 THEN 'YOUNG'
        WHEN age < 30 THEN 'PRIME'
        WHEN age < 35 THEN 'EXPERIENCED'
        ELSE 'VETERAN'
    END AS age_group
FROM RESULTS_V;

SELECT * FROM FACT_RESULTS_V WHERE ROWNUM <= 10;

SELECT 'DIM_ATHLETE'  AS dim, COUNT(*) AS rows FROM DIM_ATHLETE_V UNION ALL
SELECT 'DIM_GAME',     COUNT(*) FROM DIM_GAME_V     UNION ALL
SELECT 'DIM_EVENT',    COUNT(*) FROM DIM_EVENT_V    UNION ALL
SELECT 'DIM_COUNTRY',  COUNT(*) FROM DIM_COUNTRY_V  UNION ALL
SELECT 'FACT_RESULTS', COUNT(*) FROM FACT_RESULTS_V;
