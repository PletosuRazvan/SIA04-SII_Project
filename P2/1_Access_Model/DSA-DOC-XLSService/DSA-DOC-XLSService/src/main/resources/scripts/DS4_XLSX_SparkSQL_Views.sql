----------------------------------------------------------------------------------
--- DSA_XLSX_SparkSQL_Views.sql
----------------------------------------------------------------------------------
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8094/DSA-DOC-XLSService/rest/customers/CustomerTurnoverCategoryView');

SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8094/DSA-DOC-XLSService/rest/customers/CustomerEmployeesCategoryView');

SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8094/DSA-DOC-XLSService/rest/customers/TimePeriodView');

SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8094/DSA-DOC-XLSService/rest/customers/ProductCategoryHierarchyView');

----------------------------------------------------------------------------------
-- 1. Get Data Source JSON Schema
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8094/DSA-DOC-XLSService/rest/customers/CustomerEmployeesCategoryView');

SELECT schema_of_json('[
	{"categoryCode":"E1","categoryName":"Small","lowerBound":1.0,"upperBound":99.0}
    ]');

-- 2. Create Remote View
-- DROP VIEW CTG_CUST_EMP_VIEW;
CREATE OR REPLACE VIEW CTG_CUST_EMP_VIEW AS
WITH json_view AS (
    SELECT from_json(json_raw.data,
                     'ARRAY<STRUCT<categoryCode: STRING, categoryName: STRING, lowerBound: DOUBLE, upperBound: DOUBLE>>') array
    FROM (SELECT java_method('org.spark.service.rest.QueryRESTDataService', 'getRESTDataDocument',
        'http://localhost:8094/DSA-DOC-XLSService/rest/customers/CustomerEmployeesCategoryView')
        as data) json_raw
)
select v.*
FROM json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM CTG_CUST_EMP_VIEW;

----------------------------------------------------------------------------------
-- 1. Get Data Source JSON Schema
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8094/DSA-DOC-XLSService/rest/customers/CustomerTurnoverCategoryView');

SELECT schema_of_json('[
	{"categoryCode":"A","categoryName":"First Category","lowerBound":0.0,"upperBound":99999.0}
    ]');

-- 2. Create Remote View
-- DROP VIEW CTG_CUST_TO_VIEW;
CREATE OR REPLACE VIEW CTG_CUST_TO_VIEW AS
WITH json_view AS (
    SELECT from_json(json_raw.data,
                     'ARRAY<STRUCT<categoryCode: STRING, categoryName: STRING, lowerBound: DOUBLE, upperBound: DOUBLE>>') array
    FROM (SELECT java_method('org.spark.service.rest.QueryRESTDataService', 'getRESTDataDocument',
        'http://localhost:8094/DSA-DOC-XLSService/rest/customers/CustomerTurnoverCategoryView')
        as data) json_raw
)
select v.*
FROM json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM CTG_CUST_TO_VIEW;


----------------------------------------------------------------------------------
-- 1. Get Data Source JSON Schema
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8094/DSA-DOC-XLSService/rest/customers/TimePeriodView');

SELECT schema_of_json('[
	{"period":"T1","startDate":"2025-04-24T08:12:15.318+00:00","endDate":"2025-04-24T08:12:15.319+00:00"}
    ]');

-- 2. Create Remote View
-- DROP VIEW Periods_VIEW;
CREATE OR REPLACE VIEW Periods_VIEW AS
WITH json_view AS (
    SELECT from_json(json_raw.data,
                     'ARRAY<STRUCT<period: STRING, endDate: DATE, startDate: DATE>>') array
    FROM (SELECT java_method('org.spark.service.rest.QueryRESTDataService', 'getRESTDataDocument',
        'http://localhost:8094/DSA-DOC-XLSService/rest/customers/TimePeriodView')
        as data) json_raw
)
select v.*
FROM json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM Periods_VIEW;

----------------------------------------------------------------------------------
-- 1. Get Data Source JSON Schema
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8094/DSA-DOC-XLSService/rest/customers/ProductCategoryHierarchyView');

SELECT schema_of_json('[
	{"categoryCode":"CTG_1","categoryName":"First_","parentCategory":"Super_A"}
    ]');

-- 2. Create Remote View
-- DROP VIEW CTG_PROD_VIEW;
CREATE OR REPLACE VIEW CTG_PROD_VIEW AS
WITH json_view AS (
    SELECT from_json(json_raw.data,
                     'ARRAY<STRUCT<categoryCode: STRING, categoryName: STRING, parentCategory: STRING>>') array
    FROM (SELECT java_method('org.spark.service.rest.QueryRESTDataService', 'getRESTDataDocument',
        'http://localhost:8094/DSA-DOC-XLSService/rest/customers/ProductCategoryHierarchyView')
        as data) json_raw
)
select v.*
FROM json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM CTG_PROD_VIEW;



