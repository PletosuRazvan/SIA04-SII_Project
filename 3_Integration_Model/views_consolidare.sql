CREATE OR REPLACE VIEW vw_consolidare_olimpica AS
SELECT
    a.ID AS Athlete_ID, a.Name AS Athlete_Name, a.Sex, a.Height,
    jt.Sport, jt.Event, jt.Medal,
    g.Year, g.Season, g.City, 
    p."region" AS Country_Name
FROM athletes a
CROSS JOIN ext_results_json ej
CROSS JOIN JSON_TABLE(ej.json_content, '$[*]'
    COLUMNS (
        Athlete_ID NUMBER PATH '$.Athlete_ID',
        Games      VARCHAR2(100) PATH '$.Games',
        NOC        VARCHAR2(10)  PATH '$.NOC',
        Sport      VARCHAR2(100) PATH '$.Sport',
        Event      VARCHAR2(200) PATH '$.Event',
        Medal      VARCHAR2(20)  PATH '$.Medal'
    )
) jt
JOIN ext_games g ON jt.Games = g.Games
JOIN "noc_regions"@postgres_link p ON jt.NOC = p."noc"
WHERE a.ID = jt.Athlete_ID;

CREATE OR REPLACE VIEW vw_cube_tara_sport AS
SELECT 
    NVL(Country_Name, 'TOATE TARILE') AS Tara,
    NVL(Sport, 'TOATE SPORTURILE') AS Sport,
    COUNT(*) AS Nr_Medalii
FROM vw_consolidare_olimpica WHERE Medal != 'NA'
GROUP BY CUBE(Country_Name, Sport);

CREATE OR REPLACE VIEW vw_rollup_timp AS
SELECT 
    Year, Season, City, 
    COUNT(DISTINCT Athlete_ID) AS Nr_Participanti
FROM vw_consolidare_olimpica
GROUP BY ROLLUP(Year, Season, City);

CREATE OR REPLACE VIEW vw_grouping_sets_distributie AS
SELECT 
    Sex, Medal, 
    COUNT(*) AS Total
FROM vw_consolidare_olimpica
GROUP BY GROUPING SETS(Sex, Medal);