----------------------------------------------------------------------------------
--- DS_SQL_PG_SparkSQL_Views.sql
--- Creates Spark SQL views from DSA-SQL-JPAService REST endpoints (PostgreSQL)
--- NOC Regions data from the Olympics dataset
----------------------------------------------------------------------------------

-- 1. Test REST connectivity
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8091/DSA_SQL_JPAService/rest/olympics/NocRegionView');

----------------------------------------------------------------------------------
-- 2. Create NOC_REGIONS_VIEW from REST
SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'NOC_REGIONS_JSON_VIEW',
               'http://localhost:8091/DSA_SQL_JPAService/rest/olympics/NocRegionView');

SELECT * FROM NOC_REGIONS_JSON_VIEW;

-- DROP VIEW NOC_REGIONS_VIEW;
CREATE OR REPLACE VIEW NOC_REGIONS_VIEW AS
SELECT v.*
FROM NOC_REGIONS_JSON_VIEW AS json_view LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM NOC_REGIONS_VIEW;

