-- ============================================================
-- CONFIGURARE DB LINK HETEROGEN: Oracle -> PostgreSQL
-- ============================================================
-- 
-- NOTA: Acest setup necesita Oracle XE/EE instalat LOCAL
-- cu drepturi de admin. Nu functioneaza pe Oracle Cloud
-- Autonomous DB (nu suporta heterogeneous gateways).
--
-- Am folosit in schimb ETL via Python (load_to_oracle.py)
-- care face acelasi lucru: aduce datele din PG in Oracle.
--
-- ============================================================
-- PASI DE CONFIGURARE (necesita admin OS):
-- ============================================================
--
-- 1. Instaleaza ODBC Driver PostgreSQL (psqlodbc):
--    https://www.postgresql.org/ftp/odbc/versions/
--
-- 2. Configureaza ODBC Data Source (Windows):
--    Control Panel > ODBC Data Sources > System DSN > Add
--    - Name: pg_olympics
--    - Server: localhost
--    - Port: 5432
--    - Database: olympics_db
--    - User: olympics
--    - Password: olympics
--
-- 3. Creaza fisierul HS init: 
--    $ORACLE_HOME/hs/admin/initpg_hs.ora
--    Continut:
--      HS_FDS_CONNECT_INFO = pg_olympics
--      HS_FDS_TRACE_LEVEL = OFF
--      HS_FDS_SHAREABLE_NAME = C:\psqlodbc\psqlodbc35w.dll
--
-- 4. Adauga in listener.ora ($ORACLE_HOME/network/admin/):
--
--    SID_LIST_LISTENER =
--      (SID_LIST =
--        (SID_DESC =
--          (SID_NAME = pg_hs)
--          (ORACLE_HOME = C:\app\oracle\product\21c)
--          (PROGRAM = dg4odbc)
--        )
--      )
--
-- 5. Adauga in tnsnames.ora ($ORACLE_HOME/network/admin/):
--
--    pg_hs =
--      (DESCRIPTION =
--        (ADDRESS = (PROTOCOL = TCP)(HOST = localhost)(PORT = 1522))
--        (CONNECT_DATA = (SID = pg_hs))
--        (HS = OK)
--      )
--
-- 6. Restart listener:
--    lsnrctl stop
--    lsnrctl start
--
-- 7. Creaza DB Link (ca FDBO sau ADMIN):

CREATE DATABASE LINK pg_olympics_link
CONNECT TO "olympics" IDENTIFIED BY "olympics"
USING 'pg_hs';

-- 8. Test:
SELECT COUNT(*) FROM "athletes"@pg_olympics_link;
SELECT COUNT(*) FROM "results"@pg_olympics_link;

-- 9. Creaza view-uri federate peste DB Link:

CREATE OR REPLACE VIEW FDBO.PG_ATHLETES_V AS
SELECT 
    "athlete_id" AS athlete_id,
    "name"       AS full_name,
    "sex"        AS sex,
    "height"     AS height_cm,
    "weight"     AS weight_kg,
    'PostgreSQL/DBLink' AS data_source
FROM "athletes"@pg_olympics_link;

CREATE OR REPLACE VIEW FDBO.PG_RESULTS_V AS
SELECT 
    "result_id"  AS result_id,
    "athlete_id" AS athlete_id,
    "game_id"    AS game_id,
    "event_id"   AS event_id,
    "medal"      AS medal,
    "age"        AS age,
    "team"       AS team,
    "noc"        AS noc,
    'PostgreSQL/DBLink' AS data_source
FROM "results"@pg_olympics_link;

-- 10. Query federativ complet (PG via DBLink + Oracle local):

SELECT 
    a."name"     AS athlete_name,
    a."sex"      AS sex,
    r."medal"    AS medal,
    g.games_name AS games,
    g.year       AS year,
    s.sport_name AS sport,
    e.event_name AS event,
    c.region     AS country
FROM "results"@pg_olympics_link r
JOIN "athletes"@pg_olympics_link a ON r."athlete_id" = a."athlete_id"
LEFT JOIN OLY_REF.GAMES g         ON r."game_id"    = g.game_id
LEFT JOIN OLY_REF.EVENTS e        ON r."event_id"   = e.event_id
LEFT JOIN OLY_REF.SPORTS s        ON e.sport_id     = s.sport_id
LEFT JOIN OLY_REF.COUNTRIES c     ON r."noc"        = c.noc
WHERE r."medal" = 'Gold'
FETCH FIRST 20 ROWS ONLY;
