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

----------------------------------------------------------------------------------
-- 1. Get Data Source JSON Schema
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8091/DSA_SQL_JPAService/rest/sales/SalesView');

SELECT schema_of_json('[{"invoiceId":2101,"customerId":1001,"customerName":"Alfa SRL","invoiceDate":"2023-07-01",
						"productCode":3001,"prodName":"Prod A","quantity":20,"unitPrice":155}]');

-- 2. Create Remote View
-- DROP VIEW sales_view;
CREATE OR REPLACE VIEW sales_view AS
WITH json_view AS (
    SELECT from_json(json_raw.data,
                     'ARRAY<STRUCT<customerId: BIGINT, customerName: STRING, invoiceDate: STRING, invoiceId: BIGINT,
					  prodName: STRING, productCode: BIGINT, quantity: BIGINT, unitPrice: BIGINT>>') array
    FROM (SELECT java_method('org.spark.service.rest.QueryRESTDataService', 'getRESTDataDocument',
        'http://localhost:8091/DSA_SQL_JPAService/rest/sales/SalesView')
        as data) json_raw
)
select v.*
FROM json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM sales_view;

----------------------------------------------------------------------------------
-- 1. Get Data Source JSON Schema
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8091/DSA_SQL_JPAService/rest/sales/ProductView');

SELECT schema_of_json('[{"productCode":3001,"basePrice":155,"prodCategory":"CTG_1","prodName":"Prod A"}]');

-- 2. Create Remote View
-- DROP VIEW products_view;
CREATE OR REPLACE VIEW products_view AS
WITH json_view AS (
    SELECT from_json(json_raw.data,
                     'ARRAY<STRUCT<productCode: BIGINT, prodName: STRING, basePrice: BIGINT, prodCategory: STRING>>') array
    FROM (SELECT java_method('org.spark.service.rest.QueryRESTDataService', 'getRESTDataDocument',
        'http://localhost:8091/DSA_SQL_JPAService/rest/sales/ProductView')
        as data) json_raw
)
select v.*
FROM json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM products_view;

----------------------------------------------------------------------------------
-- 1. Get Data Source JSON Schema
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8091/DSA_SQL_JPAService/rest/sales/SalesView');

SELECT schema_of_json('[{"invoiceId":2101,"invoiceDate":"2023-06-30T21:00:00.000+00:00","customerId":1001,"customerName":"Alfa SRL"}]');

-- 2. Create Remote View
-- DROP VIEW invoices_view;
CREATE OR REPLACE VIEW invoices_view AS
WITH json_view AS (
    SELECT from_json(json_raw.data,
                     'ARRAY<STRUCT<invoiceDate: DATE, invoiceId: BIGINT, customerId: BIGINT, customerName: STRING>>') array
    FROM (SELECT java_method('org.spark.service.rest.QueryRESTDataService', 'getRESTDataDocument',
        'http://localhost:8091/DSA_SQL_JPAService/rest/sales/SalesView')
        as data) json_raw
)
select v.*
FROM json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM invoices_view;
