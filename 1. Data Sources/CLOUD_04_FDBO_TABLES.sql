CREATE TABLE FDBO.PG_ATHLETES (
    athlete_id   NUMBER(10)    PRIMARY KEY,
    full_name    VARCHAR2(300) NOT NULL,
    sex          VARCHAR2(1),
    birth_year   NUMBER(4),
    birth_place  VARCHAR2(200),
    height_cm    NUMBER(5,1),
    weight_kg    NUMBER(5,1),
    noc          VARCHAR2(10),
    data_source  VARCHAR2(20) DEFAULT 'PostgreSQL'
);

CREATE TABLE FDBO.PG_RESULTS (
    result_id    NUMBER(10)    PRIMARY KEY,
    athlete_id   NUMBER(10),
    game_id      NUMBER(10),
    event_id     NUMBER(10),
    medal        VARCHAR2(10),
    age          NUMBER(3),
    team         VARCHAR2(200),
    noc          VARCHAR2(10),
    data_source  VARCHAR2(20) DEFAULT 'PostgreSQL',
    CONSTRAINT FK_PG_RES_ATHLETE
        FOREIGN KEY (athlete_id) REFERENCES FDBO.PG_ATHLETES(athlete_id)
);

CREATE INDEX FDBO.IDX_PG_RES_ATHLETE ON FDBO.PG_RESULTS(athlete_id);
CREATE INDEX FDBO.IDX_PG_RES_GAME    ON FDBO.PG_RESULTS(game_id);
CREATE INDEX FDBO.IDX_PG_RES_EVENT   ON FDBO.PG_RESULTS(event_id);
CREATE INDEX FDBO.IDX_PG_RES_MEDAL   ON FDBO.PG_RESULTS(medal);
CREATE INDEX FDBO.IDX_PG_ATH_NOC     ON FDBO.PG_ATHLETES(noc);

CREATE TABLE FDBO.JSON_MEDAL_DOCS (
    doc_id       NUMBER(10) GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    doc_type     VARCHAR2(30)  NOT NULL,  -- 'athlete_medal' sau 'game_summary'
    json_data    CLOB CHECK (json_data IS JSON),
    created_at   TIMESTAMP DEFAULT SYSTIMESTAMP,
    data_source  VARCHAR2(20) DEFAULT 'MongoDB/JSON'
);

CREATE INDEX FDBO.IDX_JSON_DOC_TYPE ON FDBO.JSON_MEDAL_DOCS(doc_type);

CREATE TABLE FDBO.CSV_ATHLETE_EVENTS (
    row_id         NUMBER(10) GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    athlete_name   VARCHAR2(300),
    sex            VARCHAR2(1),
    age            NUMBER(3),
    height         NUMBER(5,1),
    weight         NUMBER(5,1),
    team           VARCHAR2(200),
    noc            VARCHAR2(10),
    games          VARCHAR2(50),
    year           NUMBER(4),
    season         VARCHAR2(10),
    city           VARCHAR2(100),
    sport          VARCHAR2(100),
    event          VARCHAR2(200),
    medal          VARCHAR2(10),
    region         VARCHAR2(200),
    data_source    VARCHAR2(20) DEFAULT 'CSV'
);

CREATE INDEX FDBO.IDX_CSV_NOC    ON FDBO.CSV_ATHLETE_EVENTS(noc);
CREATE INDEX FDBO.IDX_CSV_YEAR   ON FDBO.CSV_ATHLETE_EVENTS(year);
CREATE INDEX FDBO.IDX_CSV_MEDAL  ON FDBO.CSV_ATHLETE_EVENTS(medal);
CREATE INDEX FDBO.IDX_CSV_SPORT  ON FDBO.CSV_ATHLETE_EVENTS(sport);

SELECT table_name, num_rows 
FROM all_tables 
WHERE owner = 'FDBO' 
ORDER BY table_name;
