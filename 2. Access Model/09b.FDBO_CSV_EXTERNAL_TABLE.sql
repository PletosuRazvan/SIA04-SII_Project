DROP DIRECTORY CSV_DATA_DIR;

CREATE OR REPLACE DIRECTORY CSV_DATA_DIR AS 'C:\fdbo_data\csv_data';

GRANT READ, WRITE ON DIRECTORY CSV_DATA_DIR TO FDBO;

SELECT directory_name, directory_path
FROM all_directories
WHERE directory_name = 'CSV_DATA_DIR';

CREATE TABLE FDBO.EXT_ATHLETE_EVENTS (
    athlete_id    NUMBER,
    name          VARCHAR2(300),
    sex           VARCHAR2(5),
    age           NUMBER,
    height        NUMBER,
    weight        NUMBER,
    team          VARCHAR2(200),
    noc           VARCHAR2(10),
    country       VARCHAR2(200),
    games_name    VARCHAR2(50),
    year          NUMBER,
    season        VARCHAR2(10),
    city          VARCHAR2(100),
    sport_name    VARCHAR2(100),
    event_name    VARCHAR2(200),
    medal         VARCHAR2(10),
    game_id       NUMBER,
    event_id      NUMBER,
    sport_id      NUMBER
)
ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY CSV_DATA_DIR
    ACCESS PARAMETERS (
        RECORDS DELIMITED BY NEWLINE
        SKIP 1
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
        MISSING FIELD VALUES ARE NULL
        (
            athlete_id,
            name,
            sex,
            age,
            height,
            weight,
            team,
            noc,
            country,
            games_name,
            year,
            season,
            city,
            sport_name,
            event_name,
            medal,
            game_id,
            event_id,
            sport_id
        )
    )
    LOCATION ('athlete_events_enriched.csv')
)
REJECT LIMIT UNLIMITED;

SELECT COUNT(*) FROM FDBO.EXT_ATHLETE_EVENTS;
SELECT * FROM FDBO.EXT_ATHLETE_EVENTS WHERE ROWNUM <= 10;

CREATE OR REPLACE VIEW FDBO.CSV_ATHLETE_EVENTS_V AS
SELECT
    athlete_id,
    name          AS athlete_name,
    sex,
    age,
    height,
    weight,
    team,
    noc,
    country,
    games_name,
    year,
    season,
    city,
    sport_name,
    event_name,
    medal,
    game_id,
    event_id,
    sport_id
FROM FDBO.EXT_ATHLETE_EVENTS;

SELECT COUNT(*) AS total_rows FROM FDBO.CSV_ATHLETE_EVENTS_V;
SELECT * FROM FDBO.CSV_ATHLETE_EVENTS_V WHERE medal != 'None' AND ROWNUM <= 20;
