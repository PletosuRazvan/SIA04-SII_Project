CREATE OR REPLACE VIEW FDBO.OLAP_ROLLUP_COUNTRY_SPORT_V AS
SELECT 
    c.region                                  AS country_name,
    s.sport_name,
    COUNT(*)                                  AS participations,
    SUM(CASE WHEN r.medal IS NOT NULL THEN 1 ELSE 0 END) AS total_medals,
    SUM(CASE WHEN r.medal = 'Gold'   THEN 1 ELSE 0 END)  AS gold,
    SUM(CASE WHEN r.medal = 'Silver' THEN 1 ELSE 0 END)  AS silver,
    SUM(CASE WHEN r.medal = 'Bronze' THEN 1 ELSE 0 END)  AS bronze,
    GROUPING(c.region)                        AS is_country_total,
    GROUPING(s.sport_name)                    AS is_sport_total
FROM FDBO.PG_RESULTS r
JOIN OLY_REF.COUNTRIES c ON r.noc = c.noc
JOIN OLY_REF.EVENTS e    ON r.event_id = e.event_id
JOIN OLY_REF.SPORTS s    ON e.sport_id = s.sport_id
WHERE r.medal IS NOT NULL
GROUP BY ROLLUP(c.region, s.sport_name)
ORDER BY c.region NULLS LAST, s.sport_name NULLS LAST;

CREATE OR REPLACE VIEW FDBO.OLAP_ROLLUP_SEASON_YEAR_V AS
SELECT 
    g.season,
    g.year,
    g.city,
    COUNT(DISTINCT r.athlete_id)              AS unique_athletes,
    COUNT(*)                                  AS participations,
    SUM(CASE WHEN r.medal IS NOT NULL THEN 1 ELSE 0 END) AS total_medals,
    GROUPING(g.season)                        AS is_season_total,
    GROUPING(g.year)                          AS is_year_total,
    GROUPING(g.city)                          AS is_city_total
FROM FDBO.PG_RESULTS r
JOIN OLY_REF.GAMES g ON r.game_id = g.game_id
GROUP BY ROLLUP(g.season, g.year, g.city)
ORDER BY g.season NULLS LAST, g.year NULLS LAST;

CREATE OR REPLACE VIEW FDBO.OLAP_ROLLUP_SEX_AGE_V AS
SELECT 
    a.sex,
    CASE 
        WHEN r.age < 18 THEN 'Junior (<18)'
        WHEN r.age BETWEEN 18 AND 25 THEN 'Tânăr (18-25)'
        WHEN r.age BETWEEN 26 AND 35 THEN 'Matur (26-35)'
        WHEN r.age > 35 THEN 'Veteran (>35)'
        ELSE 'Necunoscut'
    END AS age_group,
    COUNT(*)                                  AS participations,
    SUM(CASE WHEN r.medal IS NOT NULL THEN 1 ELSE 0 END) AS total_medals,
    SUM(CASE WHEN r.medal = 'Gold'   THEN 1 ELSE 0 END)  AS gold,
    ROUND(AVG(r.age), 1)                      AS avg_age,
    GROUPING(a.sex)                           AS is_sex_total,
    GROUPING(CASE 
        WHEN r.age < 18 THEN 'Junior (<18)'
        WHEN r.age BETWEEN 18 AND 25 THEN 'Tânăr (18-25)'
        WHEN r.age BETWEEN 26 AND 35 THEN 'Matur (26-35)'
        WHEN r.age > 35 THEN 'Veteran (>35)'
        ELSE 'Necunoscut'
    END)                                      AS is_age_total
FROM FDBO.PG_RESULTS r
JOIN FDBO.PG_ATHLETES a ON r.athlete_id = a.athlete_id
GROUP BY ROLLUP(a.sex, 
    CASE 
        WHEN r.age < 18 THEN 'Junior (<18)'
        WHEN r.age BETWEEN 18 AND 25 THEN 'Tânăr (18-25)'
        WHEN r.age BETWEEN 26 AND 35 THEN 'Matur (26-35)'
        WHEN r.age > 35 THEN 'Veteran (>35)'
        ELSE 'Necunoscut'
    END)
ORDER BY a.sex NULLS LAST;

CREATE OR REPLACE VIEW FDBO.OLAP_CUBE_COUNTRY_SEASON_V AS
SELECT 
    c.region                                  AS country_name,
    g.season,
    COUNT(DISTINCT r.athlete_id)              AS unique_athletes,
    SUM(CASE WHEN r.medal IS NOT NULL THEN 1 ELSE 0 END) AS total_medals,
    SUM(CASE WHEN r.medal = 'Gold'   THEN 1 ELSE 0 END)  AS gold,
    SUM(CASE WHEN r.medal = 'Silver' THEN 1 ELSE 0 END)  AS silver,
    SUM(CASE WHEN r.medal = 'Bronze' THEN 1 ELSE 0 END)  AS bronze,
    GROUPING(c.region)                        AS is_country_total,
    GROUPING(g.season)                        AS is_season_total
FROM FDBO.PG_RESULTS r
JOIN OLY_REF.COUNTRIES c ON r.noc = c.noc
JOIN OLY_REF.GAMES g     ON r.game_id = g.game_id
WHERE r.medal IS NOT NULL
GROUP BY CUBE(c.region, g.season)
ORDER BY c.region NULLS LAST, g.season NULLS LAST;

CREATE OR REPLACE VIEW FDBO.OLAP_CUBE_SEX_MEDAL_V AS
SELECT 
    a.sex,
    r.medal                                   AS medal_type,
    COUNT(*)                                  AS medal_count,
    COUNT(DISTINCT r.athlete_id)              AS unique_medalists,
    COUNT(DISTINCT r.noc)                     AS countries_count,
    GROUPING(a.sex)                           AS is_sex_total,
    GROUPING(r.medal)                         AS is_medal_total
FROM FDBO.PG_RESULTS r
JOIN FDBO.PG_ATHLETES a ON r.athlete_id = a.athlete_id
WHERE r.medal IS NOT NULL
GROUP BY CUBE(a.sex, r.medal)
ORDER BY a.sex NULLS LAST, r.medal NULLS LAST;

CREATE OR REPLACE VIEW FDBO.OLAP_CUBE_SPORT_ERA_V AS
SELECT 
    s.sport_name,
    CASE 
        WHEN g.year < 1950 THEN 'Pre-1950'
        WHEN g.year < 1980 THEN '1950-1979'
        WHEN g.year < 2000 THEN '1980-1999'
        ELSE '2000+'
    END AS era,
    COUNT(*)                                  AS participations,
    SUM(CASE WHEN r.medal IS NOT NULL THEN 1 ELSE 0 END) AS medals,
    COUNT(DISTINCT r.noc)                     AS countries,
    GROUPING(s.sport_name)                    AS is_sport_total,
    GROUPING(CASE 
        WHEN g.year < 1950 THEN 'Pre-1950'
        WHEN g.year < 1980 THEN '1950-1979'
        WHEN g.year < 2000 THEN '1980-1999'
        ELSE '2000+'
    END)                                      AS is_era_total
FROM FDBO.PG_RESULTS r
JOIN OLY_REF.GAMES g     ON r.game_id = g.game_id
JOIN OLY_REF.EVENTS e    ON r.event_id = e.event_id
JOIN OLY_REF.SPORTS s    ON e.sport_id = s.sport_id
GROUP BY CUBE(s.sport_name, 
    CASE 
        WHEN g.year < 1950 THEN 'Pre-1950'
        WHEN g.year < 1980 THEN '1950-1979'
        WHEN g.year < 2000 THEN '1980-1999'
        ELSE '2000+'
    END)
ORDER BY s.sport_name NULLS LAST;

CREATE OR REPLACE VIEW FDBO.OLAP_GS_COUNTRY_SPORT_SEASON_V AS
SELECT 
    c.region                                  AS country_name,
    s.sport_name,
    g.season,
    COUNT(*)                                  AS participations,
    SUM(CASE WHEN r.medal IS NOT NULL THEN 1 ELSE 0 END) AS medals,
    GROUPING(c.region)                        AS is_country_grouped,
    GROUPING(s.sport_name)                    AS is_sport_grouped,
    GROUPING(g.season)                        AS is_season_grouped,
    GROUPING_ID(c.region, s.sport_name, g.season) AS grouping_id
FROM FDBO.PG_RESULTS r
JOIN OLY_REF.COUNTRIES c ON r.noc = c.noc
JOIN OLY_REF.GAMES g     ON r.game_id = g.game_id
JOIN OLY_REF.EVENTS e    ON r.event_id = e.event_id
JOIN OLY_REF.SPORTS s    ON e.sport_id = s.sport_id
WHERE r.medal IS NOT NULL
GROUP BY GROUPING SETS (
    (c.region),
    (s.sport_name),
    (g.season),
    ()
)
ORDER BY grouping_id, country_name NULLS LAST, sport_name NULLS LAST;

CREATE OR REPLACE VIEW FDBO.OLAP_GS_MIXED_V AS
SELECT 
    c.region                                  AS country_name,
    g.year,
    s.sport_name,
    a.sex,
    SUM(CASE WHEN r.medal IS NOT NULL THEN 1 ELSE 0 END) AS medals,
    SUM(CASE WHEN r.medal = 'Gold'   THEN 1 ELSE 0 END)  AS gold,
    GROUPING_ID(c.region, g.year, s.sport_name, a.sex) AS grouping_id
FROM FDBO.PG_RESULTS r
JOIN FDBO.PG_ATHLETES a  ON r.athlete_id = a.athlete_id
JOIN OLY_REF.COUNTRIES c ON r.noc = c.noc
JOIN OLY_REF.GAMES g     ON r.game_id = g.game_id
JOIN OLY_REF.EVENTS e    ON r.event_id = e.event_id
JOIN OLY_REF.SPORTS s    ON e.sport_id = s.sport_id
WHERE r.medal IS NOT NULL
GROUP BY GROUPING SETS (
    (c.region, g.year),     -- medalii per țară și an
    (s.sport_name, a.sex),  -- medalii per sport și sex
    (c.region),             -- total per țară
    ()                      -- grand total
)
ORDER BY grouping_id;

CREATE OR REPLACE VIEW FDBO.OLAP_GS_DATA_SOURCES_V AS
SELECT 
    source_name,
    category,
    SUM(record_count) AS record_count,
    GROUPING(source_name) AS is_source_total,
    GROUPING(category)    AS is_category_total,
    GROUPING_ID(source_name, category) AS grouping_id
FROM (
    SELECT 'DS1-Oracle' AS source_name, 'Sports'    AS category, COUNT(*) AS record_count FROM OLY_REF.SPORTS
    UNION ALL
    SELECT 'DS1-Oracle', 'Events',    COUNT(*) FROM OLY_REF.EVENTS
    UNION ALL
    SELECT 'DS1-Oracle', 'Games',     COUNT(*) FROM OLY_REF.GAMES
    UNION ALL
    SELECT 'DS1-Oracle', 'Countries', COUNT(*) FROM OLY_REF.COUNTRIES
    UNION ALL
    SELECT 'DS2-PostgreSQL', 'Athletes', COUNT(*) FROM FDBO.PG_ATHLETES
    UNION ALL
    SELECT 'DS2-PostgreSQL', 'Results',  COUNT(*) FROM FDBO.PG_RESULTS
    UNION ALL
    SELECT 'DS3-JSON/MongoDB', 'Medal Docs',   COUNT(*) FROM FDBO.JSON_MEDAL_DOCS WHERE doc_type = 'athlete_medal'
    UNION ALL
    SELECT 'DS3-JSON/MongoDB', 'Game Summaries', COUNT(*) FROM FDBO.JSON_MEDAL_DOCS WHERE doc_type = 'game_summary'
    UNION ALL
    SELECT 'DS4-CSV', 'Enriched Events', COUNT(*) FROM FDBO.CSV_ATHLETE_EVENTS
) src
GROUP BY GROUPING SETS (
    (source_name, category),
    (source_name),
    ()
)
ORDER BY grouping_id, source_name NULLS LAST, category NULLS LAST;

SELECT * FROM FDBO.OLAP_ROLLUP_COUNTRY_SPORT_V 
WHERE is_sport_total = 1 AND is_country_total = 0
ORDER BY total_medals DESC
FETCH FIRST 10 ROWS ONLY;

SELECT * FROM FDBO.OLAP_CUBE_COUNTRY_SEASON_V
WHERE is_country_total = 1 AND is_season_total = 0;

SELECT * FROM FDBO.OLAP_GS_DATA_SOURCES_V ORDER BY grouping_id, source_name;
