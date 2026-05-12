SELECT java_method('org.spark.service.rest.QueryRESTDataService','getRESTDataDocument','http://localhost:8093/DSA-NoSQL-MongoDBService/rest/olympics/ResultView');

SELECT java_method('org.spark.service.rest.RESTEnabledSQLService','createJSONViewFromREST','RESULTS_JSON_VIEW','http://localhost:8093/DSA-NoSQL-MongoDBService/rest/olympics/ResultView');

SELECT * FROM RESULTS_JSON_VIEW;

CREATE OR REPLACE VIEW RESULTS_VIEW AS
SELECT v.*
FROM RESULTS_JSON_VIEW AS json_view LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM RESULTS_VIEW;
