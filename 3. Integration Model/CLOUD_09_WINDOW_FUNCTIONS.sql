CREATE OR REPLACE VIEW FDBO.WF_TOP_MEDALISTS_V AS
SELECT 
    athlete_name,
    country_name,
    total_medals,
    gold,
    ROW_NUMBER() OVER (PARTITION BY country_name ORDER BY total_medals DESC, gold DESC) AS row_num,
    RANK()       OVER (PARTITION BY country_name ORDER BY total_medals DESC)            AS rank_in_country,
    DENSE_RANK() OVER (ORDER BY total_medals DESC)                                     AS global_dense_rank
FROM (
    SELECT 
        a.full_name AS athlete_name,
        c.region    AS country_name,
        COUNT(*)    AS total_medals,
        SUM(CASE WHEN r.medal = 'Gold' THEN 1 ELSE 0 END) AS gold
    FROM FDBO.PG_RESULTS r
    JOIN FDBO.PG_ATHLETES a  ON r.athlete_id = a.athlete_id
    JOIN OLY_REF.COUNTRIES c ON r.noc = c.noc
    WHERE r.medal IS NOT NULL
    GROUP BY a.full_name, c.region
    HAVING COUNT(*) >= 3
);

CREATE OR REPLACE VIEW FDBO.WF_RUNNING_MEDALS_V AS
SELECT 
    g.year,
    g.season,
    medals_this_edition,
    SUM(medals_this_edition) OVER (ORDER BY g.year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_medals,
    athletes_this_edition,
    SUM(athletes_this_edition) OVER (ORDER BY g.year) AS cumulative_athletes
FROM (
    SELECT 
        r.game_id,
        COUNT(CASE WHEN r.medal IS NOT NULL THEN 1 END) AS medals_this_edition,
        COUNT(DISTINCT r.athlete_id) AS athletes_this_edition
    FROM FDBO.PG_RESULTS r
    GROUP BY r.game_id
) sub
JOIN OLY_REF.GAMES g ON sub.game_id = g.game_id
ORDER BY g.year;

CREATE OR REPLACE VIEW FDBO.WF_MOVING_AVG_MEDALS_V AS
SELECT 
    g.year,
    g.season,
    g.city,
    medal_count,
    ROUND(AVG(medal_count) OVER (ORDER BY g.year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 1) AS moving_avg_3,
    ROUND(AVG(medal_count) OVER (ORDER BY g.year ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 1) AS moving_avg_5,
    MIN(medal_count) OVER (ORDER BY g.year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS min_window_3,
    MAX(medal_count) OVER (ORDER BY g.year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS max_window_3
FROM (
    SELECT 
        r.game_id,
        COUNT(CASE WHEN r.medal IS NOT NULL THEN 1 END) AS medal_count
    FROM FDBO.PG_RESULTS r
    GROUP BY r.game_id
) sub
JOIN OLY_REF.GAMES g ON sub.game_id = g.game_id
ORDER BY g.year;

CREATE OR REPLACE VIEW FDBO.WF_LAG_LEAD_EDITIONS_V AS
SELECT 
    g.year,
    g.season,
    g.city,
    total_athletes,
    LAG(total_athletes, 1) OVER (PARTITION BY g.season ORDER BY g.year)  AS prev_edition_athletes,
    LEAD(total_athletes, 1) OVER (PARTITION BY g.season ORDER BY g.year) AS next_edition_athletes,
    total_athletes - LAG(total_athletes, 1) OVER (PARTITION BY g.season ORDER BY g.year) AS growth,
    ROUND(
        (total_athletes - LAG(total_athletes, 1) OVER (PARTITION BY g.season ORDER BY g.year)) * 100.0 
        / NULLIF(LAG(total_athletes, 1) OVER (PARTITION BY g.season ORDER BY g.year), 0),
        1
    ) AS growth_pct
FROM (
    SELECT 
        r.game_id,
        COUNT(DISTINCT r.athlete_id) AS total_athletes
    FROM FDBO.PG_RESULTS r
    GROUP BY r.game_id
) sub
JOIN OLY_REF.GAMES g ON sub.game_id = g.game_id
ORDER BY g.season, g.year;

CREATE OR REPLACE VIEW FDBO.WF_ATHLETE_QUARTILES_V AS
SELECT 
    athlete_name,
    country_name,
    total_medals,
    gold,
    NTILE(4) OVER (ORDER BY total_medals DESC) AS quartile,
    PERCENT_RANK() OVER (ORDER BY total_medals) AS percent_rank,
    CUME_DIST() OVER (ORDER BY total_medals)    AS cume_dist
FROM (
    SELECT 
        a.full_name AS athlete_name,
        c.region    AS country_name,
        COUNT(*)    AS total_medals,
        SUM(CASE WHEN r.medal = 'Gold' THEN 1 ELSE 0 END) AS gold
    FROM FDBO.PG_RESULTS r
    JOIN FDBO.PG_ATHLETES a  ON r.athlete_id = a.athlete_id
    JOIN OLY_REF.COUNTRIES c ON r.noc = c.noc
    WHERE r.medal IS NOT NULL
    GROUP BY a.full_name, c.region
    HAVING COUNT(*) >= 2
);

CREATE OR REPLACE VIEW FDBO.WF_FIRST_LAST_MEDAL_V AS
SELECT DISTINCT
    s.sport_name,
    g.year,
    c.region AS country_name,
    r.medal,
    FIRST_VALUE(g.year) OVER (PARTITION BY s.sport_name ORDER BY g.year 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_medal_year,
    LAST_VALUE(g.year) OVER (PARTITION BY s.sport_name ORDER BY g.year 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_medal_year,
    FIRST_VALUE(c.region) OVER (PARTITION BY s.sport_name ORDER BY g.year
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_gold_country
FROM FDBO.PG_RESULTS r
JOIN OLY_REF.GAMES g     ON r.game_id = g.game_id
JOIN OLY_REF.EVENTS e    ON r.event_id = e.event_id
JOIN OLY_REF.SPORTS s    ON e.sport_id = s.sport_id
JOIN OLY_REF.COUNTRIES c ON r.noc = c.noc
WHERE r.medal = 'Gold';

CREATE OR REPLACE VIEW FDBO.WF_MEDAL_SHARE_V AS
SELECT 
    country_name,
    total_medals,
    gold,
    ROUND(RATIO_TO_REPORT(total_medals) OVER () * 100, 2) AS pct_of_all_medals,
    ROUND(RATIO_TO_REPORT(gold) OVER () * 100, 2)         AS pct_of_all_gold,
    RANK() OVER (ORDER BY total_medals DESC)               AS rank_total,
    RANK() OVER (ORDER BY gold DESC)                       AS rank_gold
FROM (
    SELECT 
        c.region AS country_name,
        COUNT(*) AS total_medals,
        SUM(CASE WHEN r.medal = 'Gold' THEN 1 ELSE 0 END) AS gold
    FROM FDBO.PG_RESULTS r
    JOIN OLY_REF.COUNTRIES c ON r.noc = c.noc
    WHERE r.medal IS NOT NULL
    GROUP BY c.region
)
ORDER BY total_medals DESC;

CREATE OR REPLACE VIEW FDBO.WF_COUNTRY_TOP_SPORTS_V AS
SELECT 
    country_name,
    sport_name,
    sport_medals,
    LISTAGG(sport_name, ', ') WITHIN GROUP (ORDER BY sport_medals DESC) 
        OVER (PARTITION BY country_name) AS all_sports_ranked,
    ROW_NUMBER() OVER (PARTITION BY country_name ORDER BY sport_medals DESC) AS sport_rank
FROM (
    SELECT 
        c.region    AS country_name,
        s.sport_name,
        COUNT(*)    AS sport_medals
    FROM FDBO.PG_RESULTS r
    JOIN OLY_REF.COUNTRIES c ON r.noc = c.noc
    JOIN OLY_REF.EVENTS e    ON r.event_id = e.event_id
    JOIN OLY_REF.SPORTS s    ON e.sport_id = s.sport_id
    WHERE r.medal IS NOT NULL
    GROUP BY c.region, s.sport_name
    HAVING COUNT(*) >= 10
);

SELECT * FROM FDBO.WF_TOP_MEDALISTS_V 
WHERE global_dense_rank <= 20
ORDER BY global_dense_rank;

SELECT * FROM FDBO.WF_LAG_LEAD_EDITIONS_V WHERE season = 'Summer';

SELECT * FROM FDBO.WF_MEDAL_SHARE_V WHERE rank_total <= 10;

SELECT * FROM FDBO.WF_MEDAL_SHARE_V WHERE country_name = 'Romania';
