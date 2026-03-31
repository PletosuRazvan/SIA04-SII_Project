CREATE DATABASE LINK pg_olympics_link
CONNECT TO "olympics" IDENTIFIED BY "olympics"
USING 'pg_hs';

CREATE OR REPLACE VIEW FDBO.PG_ATHLETES_REMOTE AS
SELECT * FROM "athletes"@pg_olympics_link;

CREATE OR REPLACE VIEW FDBO.PG_RESULTS_REMOTE AS
SELECT * FROM "results"@pg_olympics_link;

SELECT COUNT(*) FROM "athletes"@pg_olympics_link;
SELECT COUNT(*) FROM "results"@pg_olympics_link;

SELECT a."name", a."sex", r."medal", r."age"
FROM "athletes"@pg_olympics_link a
JOIN "results"@pg_olympics_link r ON a."athlete_id" = r."athlete_id"
WHERE r."medal" IS NOT NULL
FETCH FIRST 10 ROWS ONLY;
