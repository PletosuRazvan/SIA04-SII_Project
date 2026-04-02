CREATE TABLE ext_games (
    Games VARCHAR2(50),
    Year NUMBER,
    Season VARCHAR2(10),
    City VARCHAR2(100)
)
ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY olympics_dir
    ACCESS PARAMETERS (
        RECORDS DELIMITED BY NEWLINE
        SKIP 1
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
        MISSING FIELD VALUES ARE NULL
    )
    LOCATION ('source_games.csv')
)
REJECT LIMIT UNLIMITED;

CREATE DATABASE LINK postgres_link 
CONNECT TO "postgres" IDENTIFIED BY "postgres" 
USING 'PG_OLYMPICS';

CREATE TABLE ext_results_json (
    json_content CLOB
)
ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY olympics_dir
    ACCESS PARAMETERS (
        RECORDS DELIMITED BY 0x'0A' -- Citește tot fișierul ca un singur rând gigant
        FIELDS (json_content CHAR(100000000)) -- Rezervăm spațiu mare pentru text
    )
    LOCATION ('oracle_results.json')
)
REJECT LIMIT UNLIMITED;

CREATE OR REPLACE VIEW vw_federative_olympics AS
SELECT
    a.ID AS Athlete_ID,
    a.Name AS Athlete_Name,
    jt.Sport,
    jt.Event,
    jt.Medal,
    g.Year,
    g.City,
    p."region" AS Country_Name
FROM
    athletes a
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