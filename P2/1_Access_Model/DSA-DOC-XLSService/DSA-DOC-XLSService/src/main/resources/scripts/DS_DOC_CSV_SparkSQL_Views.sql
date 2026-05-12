----------------------------------------------------------------------------------
--- DS_DOC_CSV_SparkSQL_Views.sql
--- Creates Spark SQL views from DSA-DOC-XLSService REST endpoints (CSV data)
--- Athletes and Games data from the Olympics dataset
----------------------------------------------------------------------------------

-- 1. Test REST connectivity
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8094/DSA-DOC-XLSService/rest/olympics/AthleteView');

SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8094/DSA-DOC-XLSService/rest/olympics/GamesView');

----------------------------------------------------------------------------------
-- 2. Create ATHLETES_VIEW from REST
SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'ATHLETES_JSON_VIEW',
               'http://localhost:8094/DSA-DOC-XLSService/rest/olympics/AthleteView');

SELECT * FROM ATHLETES_JSON_VIEW;

-- DROP VIEW ATHLETES_VIEW;
CREATE OR REPLACE VIEW ATHLETES_VIEW AS
SELECT v.*
FROM ATHLETES_JSON_VIEW AS json_view LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM ATHLETES_VIEW;

----------------------------------------------------------------------------------
-- 3. Create GAMES_VIEW from REST
SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'GAMES_JSON_VIEW',
               'http://localhost:8094/DSA-DOC-XLSService/rest/olympics/GamesView');

SELECT * FROM GAMES_JSON_VIEW;

-- DROP VIEW GAMES_VIEW;
CREATE OR REPLACE VIEW GAMES_VIEW AS
SELECT v.*
FROM GAMES_JSON_VIEW AS json_view LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM GAMES_VIEW;

