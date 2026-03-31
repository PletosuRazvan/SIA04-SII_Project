CREATE OR REPLACE VIEW V_MONGO_ATHLETE_MEDALS AS
SELECT jt.*
FROM JSON_TABLE(
       get_mongo_json('http://localhost:8081/olympics_nosql/athlete_medals_docs'),
       '$[*]'
       COLUMNS (
         athlete_id    NUMBER        PATH '$.athlete_id',
         name          VARCHAR2(200) PATH '$.name',
         sex           VARCHAR2(5)   PATH '$.sex',
         total_medals  NUMBER        PATH '$.total_medals',
         gold          NUMBER        PATH '$.gold',
         silver        NUMBER        PATH '$.silver',
         bronze        NUMBER        PATH '$.bronze',
         sports_count  NUMBER        PATH '$.sports_count',
         games_count   NUMBER        PATH '$.games_count',
         first_game_id NUMBER        PATH '$.first_game_id',
         last_game_id  NUMBER        PATH '$.last_game_id'
       )
     ) jt;

CREATE OR REPLACE VIEW V_MONGO_GAME_SUMMARY AS
SELECT jt.*
FROM JSON_TABLE(
       get_mongo_json('http://localhost:8081/olympics_nosql/game_summary_docs'),
       '$[*]'
       COLUMNS (
         game_id         NUMBER         PATH '$.game_id',
         games_name      VARCHAR2(50)   PATH '$.games_name',
         year            NUMBER         PATH '$.year',
         season          VARCHAR2(20)   PATH '$.season',
         city            VARCHAR2(100)  PATH '$.city',
         total_athletes  NUMBER         PATH '$.total_athletes',
         total_entries   NUMBER         PATH '$.total_entries',
         total_medals    NUMBER         PATH '$.medals.total',
         gold_medals     NUMBER         PATH '$.medals.gold',
         silver_medals   NUMBER         PATH '$.medals.silver',
         bronze_medals   NUMBER         PATH '$.medals.bronze',
         countries_count NUMBER         PATH '$.countries_count',
         events_count    NUMBER         PATH '$.events_count'
       )
     ) jt;

SELECT COUNT(*) AS nr_medalists FROM V_MONGO_ATHLETE_MEDALS;
SELECT COUNT(*) AS nr_games     FROM V_MONGO_GAME_SUMMARY;
SELECT * FROM V_MONGO_ATHLETE_MEDALS WHERE ROWNUM <= 10;
SELECT * FROM V_MONGO_GAME_SUMMARY   WHERE ROWNUM <= 10;

SELECT
    m.name,
    m.total_medals,
    m.gold,
    g.games_name AS first_olympics,
    c.region     AS country
FROM V_MONGO_ATHLETE_MEDALS m
JOIN GAMES_V g     ON m.first_game_id = g.game_id
JOIN COUNTRIES_V c ON (
    SELECT r.noc FROM RESULTS_V r
    WHERE r.athlete_id = m.athlete_id AND ROWNUM = 1
) = c.noc
WHERE m.gold >= 3
ORDER BY m.gold DESC
FETCH FIRST 20 ROWS ONLY;
