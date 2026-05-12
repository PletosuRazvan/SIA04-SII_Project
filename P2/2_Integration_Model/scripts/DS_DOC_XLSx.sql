SELECT java_method('org.spark.service.rest.QueryRESTDataService','getRESTDataDocument','http://localhost:8094/DSA-DOC-XLSService/rest/olympics/AthleteView');

SELECT java_method('org.spark.service.rest.QueryRESTDataService','getRESTDataDocument','http://localhost:8094/DSA-DOC-XLSService/rest/olympics/GamesView');

SELECT java_method('org.spark.service.rest.RESTEnabledSQLService','createJSONViewFromREST','ATHLETES_JSON_VIEW','http://localhost:8094/DSA-DOC-XLSService/rest/olympics/AthleteView');

SELECT * FROM ATHLETES_JSON_VIEW;

CREATE OR REPLACE VIEW ATHLETES_VIEW AS
SELECT v.*
FROM ATHLETES_JSON_VIEW AS json_view LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM ATHLETES_VIEW;

SELECT java_method('org.spark.service.rest.RESTEnabledSQLService','createJSONViewFromREST','GAMES_JSON_VIEW','http://localhost:8094/DSA-DOC-XLSService/rest/olympics/GamesView');

SELECT * FROM GAMES_JSON_VIEW;

CREATE OR REPLACE VIEW GAMES_VIEW AS
SELECT v.*
FROM GAMES_JSON_VIEW AS json_view LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM GAMES_VIEW;
