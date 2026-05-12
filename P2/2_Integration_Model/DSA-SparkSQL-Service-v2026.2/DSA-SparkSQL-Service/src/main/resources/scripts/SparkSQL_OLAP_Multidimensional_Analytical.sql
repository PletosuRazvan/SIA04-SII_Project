------ Preparing ---------------------------------------------------------------
--- DSA-SQL-JPAService access to: ORCL Data Source [Sales]
--- DSA-SQL-JDBCService: PostgreSQL Data Source [Customers]
--- DSA-DOC-XLSService: Excel Data Source [Customers Categories, Periods]
--- DSA-DOC-XMLService: XML Data Source [Locations] (OR DSA-NoSQL-MongoDBService)
--------------------------------------------------------------------------------
-- Data Source Remote/External Views -------------------------------------------
-- Oracle DSA-SQL-JPAService
SELECT * FROM PRODUCTS_VIEW;
select * from INVOICES_VIEW;
select * from SALES_VIEW;
--- Excel DSA-DOC-XLSService
SELECT * FROM CTG_CUST_TO_VIEW;
SELECT * FROM CTG_CUST_EMP_VIEW;
select * from Periods_VIEW;
select * from CTG_PROD_VIEW;
--- XML DSA-DOC-XMLService | MongoDB DSA-NoSQL-MongoDBService
SELECT * FROM departaments_view;
SELECT * FROM cities_view;
SELECT * FROM departaments_cities_view;
SELECT * FROM departaments_cities_view_all;
--- PostgreSQL DSA-SQL-JDBCService
SELECT * FROM customers_view;
SELECT * FROM customers_details_view;
SELECT * FROM customers_addresses_view;
--------------------------------------------------------------------------------
--- OLAP VIEW Model
-- Dimensions
SELECT * FROM OLAP_DIM_CUSTS_CITIES_DEPTS;
SELECT * FROM olap_dim_data_calendar;
SELECT * FROM OLAP_DIM_CUST_CTG_TO;
SELECT * FROM OLAP_DIM_CUST_CTG_EMP;
-- Facts
SELECT * FROM OLAP_FACTS_SALES_AMOUNT;
-- Analytics
SELECT * FROM OLAP_VIEW_SALES_DEP_CIT_CUST;
SELECT * FROM OLAP_VIEW_SALES_CALENDAR;
SELECT * FROM OLAP_VIEW_SALES_CTG_CUST_TO;
SELECT * FROM OLAP_VIEW_SALES_CTG_CUST_EMP;
SELECT * FROM OLAP_VIEW_SALES_CTG_PROD;
SELECT * FROM OLAP_VIEW_SALES_CTG_PROD_CITIES;
--------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------
--- Dimensions
--- D1: Customers - Cities - Departaments
--DROP VIEW OLAP_DIM_CUSTS_CITIES_DEPTS;
CREATE OR REPLACE VIEW OLAP_DIM_CUSTS_CITIES_DEPTS AS
SELECT 
    C.customerid, C.name as customername, -- L1
    Ct.idcity, Ct.cityname, -- L2
    D.iddepartament, D.departamentname -- L3
FROM customers_view C 
    INNER JOIN customers_details_view CD ON c.customerid = cd.customerid
    INNER JOIN customers_addresses_view CA ON c.customerid = ca.customerid
    INNER JOIN cities_view CT ON upper(Ca.city) = upper(Ct.cityname)
    INNER JOIN departaments_cities_view DCT ON Dct.idcity =  Ct.idcity
    INNER JOIN departaments_view D ON D.iddepartament = Dct.iddepartament
;
SELECT * FROM OLAP_DIM_CUSTS_CITIES_DEPTS;
--- D2: Calendar ---------------------------------------------------------------------------------------------------------------
--DROP VIEW OLAP_DIM_DATA_CALENDAR;
CREATE OR REPLACE VIEW OLAP_DIM_DATA_CALENDAR AS
SELECT DISTINCT
    EXTRACT (year from i.invoiceDate)    as year
  , EXTRACT (month from i.invoiceDate)   as month
  , i.invoiceDate					     as day
  , p.period as t
FROM Periods_View p
INNER JOIN INVOICES_VIEW i ON 
	extract (month from i.invoicedate) 
	BETWEEN extract (month from p.startdate) and extract (month from p.enddate)
order by 3, 2, 1;
--
SELECT * FROM olap_dim_data_calendar;
-------------------------------------------------------------------------------
-- CREATE OR REPLACE VIEW OLAP_DIM_DATA_CALENDAR_ALL AS
--- D3: CUST_CTG_TO ------------------------------------------------------------
--DROP VIEW OLAP_DIM_CUST_CTG_TO;
CREATE OR REPLACE VIEW OLAP_DIM_CUST_CTG_TO AS
SELECT 
    C.CustomerId, C.Name as customername, -- L1
    T.Categorycode, 
    T.Categoryname
FROM CUSTOMERS_VIEW C  
    INNER JOIN CUSTOMERS_DETAILS_VIEW D ON C.Customerid=D.customerid
    INNER JOIN CTG_CUST_TO_VIEW T ON D.TURNOVER BETWEEN T.LowerBound and T.UpperBound
    ;

SELECT * FROM OLAP_DIM_CUST_CTG_TO;
--- D4: CUST_CTG_EMP -----------------------------------------------------------
--DROP VIEW OLAP_DIM_CUST_CTG_EMP;
CREATE OR REPLACE VIEW OLAP_DIM_CUST_CTG_EMP AS
SELECT 
    C.CustomerId, C.name as customername, -- L1
    T.Categorycode, 
    T.Categoryname
FROM CUSTOMERS_VIEW C 
    INNER JOIN CUSTOMERS_DETAILS_VIEW D ON C.CustomerId=D.CustomerId
    INNER JOIN CTG_CUST_EMP_VIEW T ON d.nrofemps BETWEEN T.LowerBound and T.UpperBound
;
SELECT * FROM OLAP_DIM_CUST_CTG_EMP;
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--- Facts
--DROP VIEW OLAP_FACTS_SALES_AMOUNT;
CREATE OR REPLACE VIEW OLAP_FACTS_SALES_AMOUNT AS
SELECT I.CustomerId, P.ProductCode, I.InvoiceDate 
    , SUM(I.Quantity * I.UnitPrice) as SALES_AMOUNT
FROM SALES_VIEW i
    INNER JOIN PRODUCTS_VIEW p ON i.PRODUCTCODE = p.PRODUCTCODE
GROUP BY I.CustomerId, P.ProductCode, I.InvoiceDate
;
SELECT * FROM OLAP_FACTS_SALES_AMOUNT;
--------------------------------------------------------------------------------
--- Analytical Views
--------------------------------------------------------------------------------
--DROP VIEW OLAP_VIEW_SALES_DEP_CIT_CUST;
CREATE OR REPLACE VIEW OLAP_VIEW_SALES_DEP_CIT_CUST AS
SELECT D.DepartamentName, D.CityName, D.CustomerName,
  SUM(NVL(f.SALES_AMOUNT, 0)) as SALES_AMOUNT    
FROM OLAP_DIM_CUSTS_CITIES_DEPTS D
    INNER JOIN OLAP_FACTS_SALES_AMOUNT F ON D.customerid = F.CustomerId
GROUP BY ROLLUP (d.DepartamentName, d.CityName, d.CustomerName)
ORDER BY 1 DESC, 2 DESC, 3 DESC;
--
SELECT * FROM OLAP_VIEW_SALES_DEP_CIT_CUST;
---
--DROP VIEW OLAP_VIEW_SALES_DEP_CIT_CUST;
CREATE OR REPLACE VIEW OLAP_VIEW_SALES_DEP_CIT_CUST AS
SELECT CASE
    WHEN D.DepartamentName IS NULL THEN '{Total General}'
    ELSE D.DepartamentName END AS departamentName,
  CASE 
    WHEN D.DepartamentName  IS NULL THEN ' '
    WHEN D.CityName  IS NULL THEN 'subtotal Departament ' || D.DepartamentName
    ELSE D.CityName END AS cityName,
  CASE 
    WHEN D.DepartamentName IS NULL THEN ' '
    WHEN D.CityName IS NULL THEN ' '
    WHEN D.CustomerName IS NULL THEN 'subtotal city ' || d.CityName
    ELSE D.CustomerName END AS customerName,
  SUM(NVL(f.SALES_AMOUNT, 0)) as sales_amount
FROM OLAP_DIM_CUSTS_CITIES_DEPTS D
    INNER JOIN OLAP_FACTS_SALES_AMOUNT F ON D.customerid = F.customerid
GROUP BY ROLLUP (d.DepartamentName, d.CityName, d.CustomerName)
ORDER BY 1, 2, 3
;
---
SELECT * FROM OLAP_VIEW_SALES_DEP_CIT_CUST;
--------------------------------------------------------------------------------------------------------------------------------
--DROP VIEW OLAP_VIEW_SALES_CALENDAR;
CREATE OR REPLACE VIEW OLAP_VIEW_SALES_CALENDAR AS
SELECT 
  CASE
    WHEN d.year IS NULL THEN '{Total General}'
    ELSE to_char(d.year, '9999') END AS year,
  CASE 
    WHEN d.year IS NULL THEN ' '
    WHEN d.month IS NULL THEN 'subtotal year ' || d.year
    ELSE to_char(d.month, '99') END AS month,
  CASE 
    WHEN d.year IS NULL THEN ' '
    WHEN d.month IS NULL THEN ' '
    WHEN d.day IS NULL THEN 'subtotal month ' || d.month
    ELSE date_format(d.day, 'dd/MM/yyyy') END AS day,    
  SUM(NVL(f.sales_amount, 0)) as sales_amount
FROM olap_dim_data_calendar D
  LEFT JOIN OLAP_FACTS_SALES_AMOUNT F ON d.day = f.InvoiceDate
GROUP BY ROLLUP (d.year, d.month, d.day)
ORDER BY 1, 2, 3
;
---
SELECT * FROM OLAP_FACTS_SALES_AMOUNT;
SELECT * FROM OLAP_DIM_DATA_CALENDAR;
SELECT * FROM OLAP_VIEW_SALES_CALENDAR;
--------------------------------------------------------------------------------------------------------------------------------
--DROP VIEW OLAP_VIEW_SALES_CTG_CUST_TO;
CREATE OR REPLACE VIEW OLAP_VIEW_SALES_CTG_CUST_TO AS
SELECT 
CASE
    WHEN D.CategoryName IS NULL THEN '{Total General}'
    ELSE D.CategoryName END AS CategoryName,
  CASE 
    WHEN D.CategoryName IS NULL THEN ' '
    WHEN D.CustomerName IS NULL THEN 'subtotal category ' || D.CategoryName
    ELSE D.CustomerName END AS CustomerName,
  SUM(NVL(f.SALES_AMOUNT, 0)) as SALES_AMOUNT    
FROM OLAP_DIM_CUST_CTG_TO D
    INNER JOIN OLAP_FACTS_SALES_AMOUNT F ON D.customerid = F.customerid
GROUP BY ROLLUP (d.CategoryName, d.CustomerName)
ORDER BY 1,2;
---
SELECT * FROM OLAP_VIEW_SALES_CTG_CUST_TO;
-------
SELECT * FROM CTG_CUST_TO_VIEW;
SELECT * FROM OLAP_DIM_CUST_CTG_TO;
SELECT * FROM OLAP_FACTS_SALES_AMOUNT;
--------------------------------------------------------------------------------------------------------------------------------
-- DROP VIEW OLAP_VIEW_SALES_CTG_CUST_EMP;
CREATE OR REPLACE VIEW OLAP_VIEW_SALES_CTG_CUST_EMP AS
SELECT 
CASE
    WHEN D.CategoryName IS NULL THEN '{Total General}'
    ELSE D.CategoryName END AS CategoryName,
  CASE 
    WHEN D.CategoryName IS NULL THEN ' '
    WHEN D.CustomerName IS NULL THEN 'subtotal category ' || D.CategoryName
    ELSE D.CustomerName END AS CustomerName,
  SUM(NVL(f.SALES_AMOUNT, 0)) as SALES_AMOUNT    
FROM OLAP_DIM_CUST_CTG_EMP D
    INNER JOIN OLAP_FACTS_SALES_AMOUNT F ON D.customerid = F.customerid
GROUP BY ROLLUP (d.CategoryName, d.CustomerName)
ORDER BY 1,2;
---
SELECT * FROM OLAP_VIEW_SALES_CTG_CUST_EMP;
-------
SELECT * FROM CTG_CUST_EMP_VIEW;
SELECT * FROM OLAP_DIM_CUST_CTG_EMP;
SELECT * FROM OLAP_FACTS_SALES_AMOUNT;
--------------------------------------------------------------------------------------------------------------------------------
-- DROP VIEW OLAP_VIEW_SALES_CTG_PROD;
CREATE OR REPLACE VIEW OLAP_VIEW_SALES_CTG_PROD AS
SELECT 
  CASE
    WHEN D.prodcategory IS NULL THEN '{Total General}'
    ELSE D.prodcategory END AS prodcategory,
  CASE 
    WHEN D.prodcategory IS NULL THEN ' '
    WHEN D.prodName IS NULL THEN 'subtotal category ' || D.prodcategory
    ELSE D.prodName END AS prodName,
  SUM(NVL(f.SALES_AMOUNT, 0)) as SALES_AMOUNT    
FROM PRODUCTS_VIEW D
    INNER JOIN OLAP_FACTS_SALES_AMOUNT F ON D.productcode = F.productcode
GROUP BY ROLLUP (d.prodcategory, d.prodName)
ORDER BY 1,2;
---
SELECT * FROM PRODUCTS_VIEW;
SELECT * FROM OLAP_FACTS_SALES_AMOUNT;
SELECT * FROM OLAP_VIEW_SALES_CTG_PROD;

--------------------------------------------------------------------------------
-- DROP VIEW OLAP_VIEW_SALES_CTG_PROD_CITIES;
CREATE OR REPLACE VIEW OLAP_VIEW_SALES_CTG_PROD_CITIES AS
SELECT D.prodcategory, c.cityname,
  SUM(NVL(f.SALES_AMOUNT, 0)) as SALES_AMOUNT    
FROM PRODUCTS_VIEW D
    INNER JOIN OLAP_FACTS_SALES_AMOUNT F ON D.productcode = F.productcode
    INNER JOIN olap_dim_custs_cities_depts C ON c.customerid = F.customerid
GROUP BY CUBE(d.prodcategory, c.cityname)
ORDER BY 1 desc, 2 desc;
---
SELECT * FROM OLAP_VIEW_SALES_CTG_PROD_CITIES;

------------------------------------------------------------------------------------------
SELECT * FROM OLAP_FACTS_SALES_AMOUNT;
SELECT * FROM PRODUCTS_VIEW;
--- Spark SQL PIVOT
SELECT * FROM (
  SELECT EXTRACT (MONTH FROM invoiceDate) as sales_mounth, p.prodName,
    f.sales_amount AS Total_Sales
  FROM OLAP_FACTS_SALES_AMOUNT f 
    INNER JOIN PRODUCTS_VIEW p ON f.productCode = p.productCode
  ORDER BY 1
 ) V
PIVOT (
  SUM(Total_Sales) 
  FOR prodName IN 
    (
    'Prod A' AS Produs_A,
    'Prod B' AS Produs_B,
    'Prod C' AS Produs_C,
    'Prod D' AS Produs_D)
  )
ORDER BY 1;
------------------------------------------------------------------------------------------
SELECT * FROM customers_details_view;
--- Spark SQL UNPIVOT
SELECT customerId, detail_label, detail_value 
    FROM (SELECT CUSTOMERID, 
            CREDITRATING, COMPTYPE, 
            TO_CHAR(AGE, '999') as AGE, 
            TO_CHAR(TURNOVER, '999999999') as TURNOVER,
            TO_CHAR(NROFEMPS, '999') as NROFEMPS
            FROM customers_details_view) 
        cdview
UNPIVOT INCLUDE NULLS(
    detail_value
    FOR detail_label IN
        (
            CREDITRATING as CREDIT_RATING,
            COMPTYPE as COMPANY_TYPE,
            AGE as AGE,
            TURNOVER as TURNOVER,
            NROFEMPS as NR_OF_EMPS
            )
    );

-----------------------------------------------------------------------------------------
SELECT * FROM OLAP_FACTS_SALES_AMOUNT;
-- Spark Analytical Window Functions
SELECT INVOICEDATE, CUSTOMERID, SALES_AMOUNT,
SUM(SALES_AMOUNT) 
    OVER(
        PARTITION BY INVOICEDATE 
        ORDER BY CUSTOMERID
        ROWS UNBOUNDED PRECEDING) AS Aggregated_Amount_UP,
SUM(SALES_AMOUNT) OVER(PARTITION BY INVOICEDATE ORDER BY CUSTOMERID 
    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS Aggregated_Amount_CRUF,
SUM(SALES_AMOUNT) OVER(PARTITION BY INVOICEDATE ORDER BY CUSTOMERID 
    ROWS 1 PRECEDING) AS Aggregated_Amount_1PCR,
SUM(SALES_AMOUNT) OVER(PARTITION BY INVOICEDATE ORDER BY CUSTOMERID 
    ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS Aggregated_Amount_CR1F,
SUM(SALES_AMOUNT) OVER(PARTITION BY INVOICEDATE ORDER BY CUSTOMERID 
    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS Aggregated_Amount_1P1F,
SUM(SALES_AMOUNT) OVER(PARTITION BY INVOICEDATE ORDER BY CUSTOMERID 
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS Aggregated_Amount_UP1F
FROM OLAP_FACTS_SALES_AMOUNT
ORDER BY 1, 2;

-----------------------------------------------------------------------------------------
SELECT * FROM OLAP_FACTS_SALES_AMOUNT;
--- Spark SQL Ranking functions

SELECT PRODUCTCODE, Product_Sales,
  RANK() OVER (ORDER BY Product_Sales DESC) AS Poz1_RANK,
  DENSE_RANK() OVER (ORDER BY Product_Sales DESC) AS Poz2_DENSE_RANK,
  PERCENT_RANK() OVER (ORDER BY Product_Sales DESC) AS Poz3_PERCENT_RANK,
  ROW_NUMBER() OVER (ORDER BY Product_Sales DESC) AS Poz4_ROW_NUMBER
  , COUNT(PRODUCTCODE) OVER (ORDER BY Product_Sales DESC ROWS UNBOUNDED PRECEDING) as Poz5_COUNT
FROM (SELECT PRODUCTCODE,SUM(SALES_AMOUNT) AS Product_Sales
      FROM OLAP_FACTS_SALES_AMOUNT
      GROUP BY PRODUCTCODE ORDER BY 2 DESC) Top_Product_Sales
ORDER BY 3,1;
---
SELECT f.*, 
    SUM(f.sales_amount) OVER(
        PARTITION BY f.CUSTOMERID
        ORDER BY f.invoicedate
        ROWS UNBOUNDED PRECEDING) aggregate_sales_amount,
    FIRST_VALUE(f.sales_amount) OVER(
        PARTITION BY f.invoicedate
        ORDER BY f.productcode
    ) first_value_over_date,
    LAST_VALUE(f.sales_amount) OVER(
        PARTITION BY f.invoicedate
        ORDER BY f.productcode
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) last_value_over_date,
    LAG(f.sales_amount) OVER(
        PARTITION BY f.invoicedate
        ORDER BY f.productcode
    ) lag_value_over_date,
    LEAD(f.sales_amount) OVER(
        PARTITION BY f.invoicedate
        ORDER BY f.productcode
    ) lead_value_over_date
FROM OLAP_FACTS_SALES_AMOUNT F
ORDER BY INVOICEDATE;

------------------------------------------------------------------------------------------
--- SparkSQL Advanced Statistical Functions
-- Central: pop cust_id
WITH cust_sales AS(
    SELECT
        CUSTOMERID,
        SUM(sales_amount) sales_amount
    FROM OLAP_FACTS_SALES_AMOUNT GROUP BY CUSTOMERID)
SELECT
    AVG(sales_amount) cust_sales_avg,
    MEDIAN(sales_amount) cust_sales_median,
    MODE(sales_amount) cust_sales_mode
FROM cust_sales;
-- Standard deviation: pop cust_id
WITH cust_sales AS(
    SELECT
        CUSTOMERID,
        SUM(sales_amount) sales_amount
    FROM OLAP_FACTS_SALES_AMOUNT GROUP BY CUSTOMERID)
SELECT
    ROUND(VARIANCE(sales_amount), 2)   AS cust_var_sample
    ,ROUND(VAR_POP(sales_amount), 2)    AS cust_var_population
    ,ROUND(STDDEV(sales_amount), 2)     AS cust_stddev_sample
    ,ROUND(STDDEV_POP(sales_amount), 2) AS cust_stddev_population
FROM cust_sales;
---
WITH cust_sales AS(
    SELECT
        CUSTOMERID,
        SUM(sales_amount) sales_amount
    FROM OLAP_FACTS_SALES_AMOUNT GROUP BY CUSTOMERID)
SELECT
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY sales_amount) AS p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY sales_amount) AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY sales_amount) AS p75
FROM cust_sales;
----
WITH prod_sales AS(
    SELECT
        productcode,
        SUM(sales_amount) sales_amount
    FROM OLAP_FACTS_SALES_AMOUNT GROUP BY productcode)
SELECT
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY sales_amount) AS p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY sales_amount) AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY sales_amount) AS p75
FROM prod_sales;

-----------------------------
-- i.  Pearson coefficient: num CUST_TURNOVER, num SALES_AMOUNT
-- Valid range [-1, +1] => [negative, no, positive] correlation
WITH VIEW_OLAP_DIM_CUST_CTG_TUROVER AS (
    SELECT D.TURNOVER, SUM(F.SALES_AMOUNT) AS SALES_AMOUNT
        FROM OLAP_FACTS_SALES_AMOUNT F INNER JOIN CUSTOMERS_DETAILS_VIEW D ON F.CUSTOMERID = D.CUSTOMERID
    GROUP BY D.TURNOVER)
SELECT CORR(TURNOVER, SALES_AMOUNT) CORRELATION_PEARSON FROM VIEW_OLAP_DIM_CUST_CTG_TUROVER;


------------------------------------------------------------------------------------------
SELECT * FROM OLAP_VIEW_SALES_DEP_CIT_CUST WHERE customerName <> ' ' and customerName NOT LIKE 'subtotal%';

SELECT categoryname, sales_amount
FROM OLAP_VIEW_SALES_CTG_CUST_EMP
WHERE customerName <> ' ' and customerName LIKE 'subtotal%';

SELECT * FROM OLAP_VIEW_SALES_CTG_CUST_TO ;

------------------------------------------------------------------------------------------
SHOW VIEWS;
SHOW TBLPROPERTIES OLAP_DIM_CUSTS_CITIES_DEPTS;

ALTER VIEW OLAP_DIM_CUSTS_CITIES_DEPTS SET TBLPROPERTIES('AUTOREST' = 'olap/dim/custs_cities_depts');
ALTER VIEW OLAP_DIM_CUSTS_CITIES_DEPTS UNSET TBLPROPERTIES ('AUTOREST');

SHOW TBLPROPERTIES OLAP_DIM_CUSTS_CITIES_DEPTS('AUTOREST');
--------------------------------------------------------------------------------
--REST Service URL:
--	http://localhost:9990/DSA-SparkSQL-Service/rest/view/{VIEW_NAME}
--	http://localhost:9990/DSA-SparkSQL-Service/rest/STRUCT/{VIEW_NAME}
-- Enable restpoint for olap_view_sales_ctg_prod:
-- http://localhost:9990/DSA-SparkSQL-Service/rest/view/OLAP_VIEW_SALES_CTG_PROD_CITIES
ALTER VIEW olap_view_sales_ctg_prod SET TBLPROPERTIES('AUTOREST' = "olap/view/sales_ctg_prod");
SHOW TBLPROPERTIES olap_view_sales_ctg_prod;
ALTER VIEW olap_view_sales_ctg_prod UNSET TBLPROPERTIES ('AUTOREST');
--- Invoke http://localhost:9990/DSA-SparkSQL-Service/rest/auto?redef=true
--- to reload VIEW defs into Spark SQL live session
--- or show REST-view with
--- http://localhost:9990/DSA-SparkSQL-Service/rest/view/olap/view/sales_ctg_prod?redef=true

