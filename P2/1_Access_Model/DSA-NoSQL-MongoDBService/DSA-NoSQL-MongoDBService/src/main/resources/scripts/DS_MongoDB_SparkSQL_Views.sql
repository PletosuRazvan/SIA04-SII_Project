----------------------------------------------------------------------------------
--- DS_MongoDB_SparkSQL_Views.sql
--- Creates Spark SQL views from DSA-NoSQL-MongoDBService REST endpoints
--- Results data (Athlete_ID, Games, NOC, Sport, Event, Medal) from MongoDB
----------------------------------------------------------------------------------

-- 1. Test REST connectivity
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8093/DSA-NoSQL-MongoDBService/rest/olympics/ResultView');

----------------------------------------------------------------------------------
-- 2. Create RESULTS_VIEW from REST
SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'RESULTS_JSON_VIEW',
               'http://localhost:8093/DSA-NoSQL-MongoDBService/rest/olympics/ResultView');

SELECT * FROM RESULTS_JSON_VIEW;

-- DROP VIEW RESULTS_VIEW;
CREATE OR REPLACE VIEW RESULTS_VIEW AS
SELECT v.*
FROM RESULTS_JSON_VIEW AS json_view LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM RESULTS_VIEW;

