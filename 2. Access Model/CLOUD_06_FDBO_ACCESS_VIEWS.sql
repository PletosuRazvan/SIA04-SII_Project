CREATE OR REPLACE VIEW FDBO.FDBO_SPORTS_V AS
SELECT sport_id, sport_name, 'Oracle/OLY_REF' AS data_source
FROM OLY_REF.SPORTS;

CREATE OR REPLACE VIEW FDBO.FDBO_EVENTS_V AS
SELECT e.event_id, e.sport_id, e.event_name, 
       s.sport_name,
       'Oracle/OLY_REF' AS data_source
FROM OLY_REF.EVENTS e
JOIN OLY_REF.SPORTS s ON e.sport_id = s.sport_id;

CREATE OR REPLACE VIEW FDBO.FDBO_GAMES_V AS
SELECT game_id, games_name, year, season, city,
       'Oracle/OLY_REF' AS data_source
FROM OLY_REF.GAMES;

CREATE OR REPLACE VIEW FDBO.FDBO_COUNTRIES_V AS
SELECT noc, region, notes,
       'Oracle/OLY_REF' AS data_source
FROM OLY_REF.COUNTRIES;

CREATE OR REPLACE VIEW FDBO.FDBO_PG_ATHLETES_V AS
SELECT athlete_id, full_name, sex, birth_year, birth_place,
       height_cm, weight_kg, noc,
       'PostgreSQL' AS data_source
FROM FDBO.PG_ATHLETES;

CREATE OR REPLACE VIEW FDBO.FDBO_PG_RESULTS_V AS
SELECT result_id, athlete_id, game_id, event_id,
       medal, age, team, noc,
       'PostgreSQL' AS data_source
FROM FDBO.PG_RESULTS;

CREATE OR REPLACE VIEW FDBO.FDBO_JSON_ATHLETE_MEDALS_V AS
SELECT 
    j.doc_id,
    jt.athlete_name,
    jt.total_medals,
    jt.gold_medals,
    jt.silver_medals,
    jt.bronze_medals,
    jt.sports_count,
    jt.games_count,
    'MongoDB/JSON' AS data_source
FROM FDBO.JSON_MEDAL_DOCS j,
     JSON_TABLE(j.json_data, '$'
         COLUMNS (
             athlete_name  VARCHAR2(300)  PATH '$.name',
             total_medals  NUMBER(5)      PATH '$.total_medals',
             gold_medals   NUMBER(5)      PATH '$.gold',
             silver_medals NUMBER(5)      PATH '$.silver',
             bronze_medals NUMBER(5)      PATH '$.bronze',
             sports_count  NUMBER(5)      PATH '$.sports_count',
             games_count   NUMBER(5)      PATH '$.games_count'
         )
     ) jt
WHERE j.doc_type = 'athlete_medal';

CREATE OR REPLACE VIEW FDBO.FDBO_JSON_GAME_SUMMARY_V AS
SELECT
    j.doc_id,
    jt.games_name,
    jt.year,
    jt.season,
    jt.city,
    jt.total_athletes,
    jt.total_entries,
    jt.countries_count,
    jt.events_count,
    'MongoDB/JSON' AS data_source
FROM FDBO.JSON_MEDAL_DOCS j,
     JSON_TABLE(j.json_data, '$'
         COLUMNS (
             games_name      VARCHAR2(50)  PATH '$.games_name',
             year            NUMBER(4)     PATH '$.year',
             season          VARCHAR2(10)  PATH '$.season',
             city            VARCHAR2(100) PATH '$.city',
             total_athletes  NUMBER(10)    PATH '$.total_athletes',
             total_entries   NUMBER(10)    PATH '$.total_entries',
             countries_count NUMBER(5)     PATH '$.countries_count',
             events_count    NUMBER(5)     PATH '$.events_count'
         )
     ) jt
WHERE j.doc_type = 'game_summary';

CREATE OR REPLACE VIEW FDBO.FDBO_CSV_EVENTS_V AS
SELECT row_id, athlete_name, sex, age, height, weight,
       team, noc, games, year, season, city,
       sport, event, medal, region,
       'CSV' AS data_source
FROM FDBO.CSV_ATHLETE_EVENTS;

SELECT view_name FROM all_views WHERE owner = 'FDBO' ORDER BY view_name;
