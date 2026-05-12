SELECT java_method('org.spark.service.rest.QueryRESTDataService','getRESTDataDocument','http://localhost:8091/DSA_SQL_JPAService/rest/olympics/NocRegionView');

SELECT java_method('org.spark.service.rest.RESTEnabledSQLService','createJSONViewFromREST','NOC_REGIONS_JSON_VIEW','http://localhost:8091/DSA_SQL_JPAService/rest/olympics/NocRegionView');

SELECT * FROM NOC_REGIONS_JSON_VIEW;

CREATE OR REPLACE VIEW NOC_REGIONS_VIEW AS
SELECT v.*
FROM NOC_REGIONS_JSON_VIEW AS json_view LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM NOC_REGIONS_VIEW;
