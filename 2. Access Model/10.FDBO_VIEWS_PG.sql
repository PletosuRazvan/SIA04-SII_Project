SHOW CON_NAME;

CREATE OR REPLACE VIEW FDBO.ATHLETES_V AS
SELECT
    "athlete_id"  AS athlete_id,
    "name"        AS name,
    "sex"         AS sex,
    "height"      AS height,
    "weight"      AS weight
FROM "athletes"@PG_LINK;

CREATE OR REPLACE VIEW FDBO.RESULTS_V AS
SELECT
    "result_id"   AS result_id,
    "athlete_id"  AS athlete_id,
    "game_id"     AS game_id,
    "event_id"    AS event_id,
    "noc"         AS noc,
    "team"        AS team,
    "age"         AS age,
    "medal"       AS medal
FROM "results"@PG_LINK;

SELECT COUNT(*) AS nr_athletes FROM FDBO.ATHLETES_V;
SELECT COUNT(*) AS nr_results  FROM FDBO.RESULTS_V;
SELECT * FROM FDBO.ATHLETES_V WHERE ROWNUM <= 10;
SELECT * FROM FDBO.RESULTS_V  WHERE ROWNUM <= 10;
