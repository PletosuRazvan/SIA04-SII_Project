CREATE OR REPLACE VIEW WV_COUNTRY_RUNNING_MEDALS_V AS
SELECT
    c.country_name,
    g.year,
    g.season,
    SUM(f.is_medalist) AS medals_this_edition,
    SUM(SUM(f.is_medalist)) OVER (
        PARTITION BY c.country_name
        ORDER BY g.year, g.season
        ROWS UNBOUNDED PRECEDING
    ) AS running_total_medals
FROM FACT_RESULTS_V f
JOIN DIM_COUNTRY_V c ON f.noc     = c.noc
JOIN DIM_GAME_V g    ON f.game_id = g.game_id
GROUP BY c.country_name, g.year, g.season
ORDER BY c.country_name, g.year, g.season;

SELECT * FROM WV_COUNTRY_RUNNING_MEDALS_V WHERE country_name = 'Romania';

CREATE OR REPLACE VIEW WV_COUNTRY_RANK_PER_GAME_V AS
SELECT
    g.games_name,
    g.year,
    c.country_name,
    SUM(f.is_gold)     AS gold_medals,
    SUM(f.is_medalist) AS total_medals,
    RANK() OVER (
        PARTITION BY g.game_id
        ORDER BY SUM(f.is_gold) DESC, SUM(f.is_medalist) DESC
    ) AS rank_gold,
    DENSE_RANK() OVER (
        PARTITION BY g.game_id
        ORDER BY SUM(f.is_medalist) DESC
    ) AS dense_rank_total,
    ROW_NUMBER() OVER (
        PARTITION BY g.game_id
        ORDER BY SUM(f.is_gold) DESC, SUM(f.is_medalist) DESC, c.country_name
    ) AS row_num
FROM FACT_RESULTS_V f
JOIN DIM_GAME_V g    ON f.game_id = g.game_id
JOIN DIM_COUNTRY_V c ON f.noc     = c.noc
WHERE f.is_medalist = 1
GROUP BY g.game_id, g.games_name, g.year, c.country_name;

SELECT * FROM WV_COUNTRY_RANK_PER_GAME_V
WHERE year = 2016 AND rank_gold <= 10
ORDER BY rank_gold;

CREATE OR REPLACE VIEW WV_SPORT_AVG_AGE_TREND_V AS
SELECT
    e.sport_name,
    g.decade,
    COUNT(*)          AS participations,
    ROUND(AVG(f.age), 1) AS avg_age,
    ROUND(
        AVG(AVG(f.age)) OVER (
            PARTITION BY e.sport_name
            ORDER BY g.decade
            ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        ), 1
    ) AS moving_avg_age
FROM FACT_RESULTS_V f
JOIN DIM_EVENT_V e ON f.event_id = e.event_id
JOIN DIM_GAME_V g  ON f.game_id  = g.game_id
WHERE f.age IS NOT NULL
GROUP BY e.sport_name, g.decade;

SELECT * FROM WV_SPORT_AVG_AGE_TREND_V WHERE sport_name = 'Gymnastics' ORDER BY decade;

CREATE OR REPLACE VIEW WV_COUNTRY_MEDAL_DIFF_AVG_V AS
SELECT
    c.country_name,
    g.games_name,
    g.year,
    SUM(f.is_medalist) AS medals_this_game,
    ROUND(
        AVG(SUM(f.is_medalist)) OVER (
            PARTITION BY c.country_name
        ), 1
    ) AS avg_medals_per_game,
    ROUND(
        SUM(f.is_medalist) - AVG(SUM(f.is_medalist)) OVER (
            PARTITION BY c.country_name
        ), 1
    ) AS diff_from_avg
FROM FACT_RESULTS_V f
JOIN DIM_COUNTRY_V c ON f.noc     = c.noc
JOIN DIM_GAME_V g    ON f.game_id = g.game_id
WHERE f.is_medalist = 1
GROUP BY c.country_name, g.game_id, g.games_name, g.year;

SELECT * FROM WV_COUNTRY_MEDAL_DIFF_AVG_V WHERE country_name = 'United States' ORDER BY year;

CREATE OR REPLACE VIEW WV_COUNTRY_FIRST_LAST_MEDAL_V AS
SELECT
    c.country_name,
    g.games_name,
    g.year,
    SUM(f.is_medalist) AS medals,
    FIRST_VALUE(g.games_name) OVER (
        PARTITION BY c.country_name
        ORDER BY g.year, g.games_name
    ) AS first_medal_games,
    LAST_VALUE(g.games_name) OVER (
        PARTITION BY c.country_name
        ORDER BY g.year, g.games_name
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS last_medal_games,
    ROW_NUMBER() OVER (
        PARTITION BY c.country_name
        ORDER BY SUM(f.is_medalist) DESC, g.year
    ) AS best_edition_rank
FROM FACT_RESULTS_V f
JOIN DIM_COUNTRY_V c ON f.noc     = c.noc
JOIN DIM_GAME_V g    ON f.game_id = g.game_id
WHERE f.is_medalist = 1
GROUP BY c.country_name, g.game_id, g.games_name, g.year;

SELECT * FROM WV_COUNTRY_FIRST_LAST_MEDAL_V
WHERE country_name = 'Romania'
ORDER BY year;

CREATE OR REPLACE VIEW WV_SPORT_MEDAL_SHARE_V AS
SELECT
    g.games_name,
    g.year,
    e.sport_name,
    SUM(f.is_medalist) AS sport_medals,
    SUM(SUM(f.is_medalist)) OVER (
        PARTITION BY g.game_id
    ) AS total_game_medals,
    ROUND(
        100.0 * SUM(f.is_medalist) / SUM(SUM(f.is_medalist)) OVER (
            PARTITION BY g.game_id
        ), 2
    ) AS pct_of_game_medals,
    SUM(SUM(f.is_medalist)) OVER (
        PARTITION BY e.sport_name
        ORDER BY g.year
        ROWS UNBOUNDED PRECEDING
    ) AS running_total_sport
FROM FACT_RESULTS_V f
JOIN DIM_EVENT_V e ON f.event_id = e.event_id
JOIN DIM_GAME_V g  ON f.game_id  = g.game_id
WHERE f.is_medalist = 1
GROUP BY g.game_id, g.games_name, g.year, e.sport_name;

SELECT * FROM WV_SPORT_MEDAL_SHARE_V WHERE year = 2016 ORDER BY pct_of_game_medals DESC;

CREATE OR REPLACE VIEW WV_ATHLETE_SPORT_RANK_V AS
SELECT
    x.sport_name,
    x.athlete_name,
    x.country_name,
    x.total_medals,
    x.gold_medals,
    RANK() OVER (
        PARTITION BY x.sport_name
        ORDER BY x.total_medals DESC
    ) AS rank_in_sport,
    DENSE_RANK() OVER (
        PARTITION BY x.sport_name
        ORDER BY x.gold_medals DESC
    ) AS gold_rank_in_sport,
    ROW_NUMBER() OVER (
        PARTITION BY x.sport_name
        ORDER BY x.total_medals DESC, x.gold_medals DESC, x.athlete_name
    ) AS row_num_in_sport
FROM (
    SELECT
        e.sport_name,
        a.name         AS athlete_name,
        c.country_name,
        SUM(f.is_medalist) AS total_medals,
        SUM(f.is_gold)     AS gold_medals
    FROM FACT_RESULTS_V f
    JOIN DIM_EVENT_V e   ON f.event_id   = e.event_id
    JOIN DIM_ATHLETE_V a ON f.athlete_id = a.athlete_id
    JOIN DIM_COUNTRY_V c ON f.noc        = c.noc
    WHERE f.is_medalist = 1
    GROUP BY e.sport_name, a.name, c.country_name
) x;

SELECT * FROM WV_ATHLETE_SPORT_RANK_V
WHERE sport_name = 'Swimming' AND rank_in_sport <= 10
ORDER BY rank_in_sport;

CREATE OR REPLACE VIEW WV_COUNTRY_LAG_LEAD_V AS
SELECT
    c.country_name,
    g.games_name,
    g.year,
    g.season,
    SUM(f.is_medalist) AS medals,
    LAG(SUM(f.is_medalist), 1, 0) OVER (
        PARTITION BY c.country_name, g.season
        ORDER BY g.year
    ) AS prev_edition_medals,
    LEAD(SUM(f.is_medalist), 1, 0) OVER (
        PARTITION BY c.country_name, g.season
        ORDER BY g.year
    ) AS next_edition_medals,
    SUM(f.is_medalist) - LAG(SUM(f.is_medalist), 1, 0) OVER (
        PARTITION BY c.country_name, g.season
        ORDER BY g.year
    ) AS change_from_prev
FROM FACT_RESULTS_V f
JOIN DIM_COUNTRY_V c ON f.noc     = c.noc
JOIN DIM_GAME_V g    ON f.game_id = g.game_id
WHERE f.is_medalist = 1
GROUP BY c.country_name, g.game_id, g.games_name, g.year, g.season;

SELECT * FROM WV_COUNTRY_LAG_LEAD_V
WHERE country_name = 'Romania' AND season = 'Summer'
ORDER BY year;
