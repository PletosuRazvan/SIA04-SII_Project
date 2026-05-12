----------------------------------------------------------------------------------
--- DS2_ORACLE_SparkSQL_Views.sql
----------------------------------------------------------------------------------
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8091/DSA_SQL_JPAService/rest/sales/SalesView');

SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8091/DSA_SQL_JPAService/rest/sales/ProductView');

SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8091/DSA_SQL_JPAService/rest/sales/SalesView');
----------------------------------------------------------------------------------
-- 1. CREATE JSON View
SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'SALES_JSON_VIEW',
               'http://localhost:8091/DSA_SQL_JPAService/rest/sales/SalesView');

SELECT * FROM SALES_JSON_VIEW;

-- 2. Create SQL View
-- DROP VIEW sales_view;
CREATE OR REPLACE VIEW sales_view AS
select v.*
FROM SALES_JSON_VIEW as json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM sales_view;

----------------------------------------------------------------------------------
-- 1. CREATE JSON View
SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'PRODUCT_JSON_VIEW',
               'http://localhost:8091/DSA_SQL_JPAService/rest/sales/ProductView');

SELECT * FROM PRODUCT_JSON_VIEW;

-- 2. Create SQL View
-- DROP VIEW products_view;
CREATE OR REPLACE VIEW products_view AS
select v.*
FROM PRODUCT_JSON_VIEW as json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM products_view;

-- DROP VIEW invoices_view;
CREATE OR REPLACE VIEW invoices_view AS
select v.invoiceDate, v.invoiceId, v.customerId, v.customerName
FROM SALES_JSON_VIEW as json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM invoices_view;
