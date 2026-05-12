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
-- 1. Create JSON View
SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'CTG_CUST_EMP_JSON_VIEW',
               'http://localhost:8094/DSA-DOC-XLSService/rest/customers/CustomerEmployeesCategoryView');

SELECT * FROM CTG_CUST_EMP_JSON_VIEW;

-- 2. Create SQL View
-- DROP VIEW CTG_CUST_EMP_VIEW;
CREATE OR REPLACE VIEW CTG_CUST_EMP_VIEW AS
select v.*
FROM CTG_CUST_EMP_JSON_VIEW as json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM CTG_CUST_EMP_VIEW;

----------------------------------------------------------------------------------
-- 1. Create JSON View
SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'CTG_CUST_TO_JSON_VIEW',
               'http://localhost:8094/DSA-DOC-XLSService/rest/customers/CustomerTurnoverCategoryView');

SELECT * FROM CTG_CUST_TO_JSON_VIEW;

-- 2. Create Remote View
-- DROP VIEW CTG_CUST_TO_VIEW;
CREATE OR REPLACE VIEW CTG_CUST_TO_VIEW AS
select v.*
FROM CTG_CUST_TO_JSON_VIEW as json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM CTG_CUST_TO_VIEW;

----------------------------------------------------------------------------------
-- 1. Create JSON View
SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'PERIODS_JSON_VIEW',
               'http://localhost:8094/DSA-DOC-XLSService/rest/customers/TimePeriodView');

SELECT * FROM PERIODS_JSON_VIEW;

-- 2. Create Remote View
-- DROP VIEW Periods_VIEW;
CREATE OR REPLACE VIEW PERIODS_VIEW AS
select v.*
FROM PERIODS_JSON_VIEW as json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM PERIODS_VIEW;

----------------------------------------------------------------------------------
-- 1. Create JSON View
SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'CTG_PROD_JSON_VIEW',
               'http://localhost:8094/DSA-DOC-XLSService/rest/customers/ProductCategoryHierarchyView');

SELECT * FROM CTG_PROD_JSON_VIEW;

-- 2. Create Remote View
-- DROP VIEW CTG_PROD_VIEW;
CREATE OR REPLACE VIEW CTG_PROD_VIEW AS
select v.*
FROM CTG_PROD_JSON_VIEW as json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM CTG_PROD_VIEW;
