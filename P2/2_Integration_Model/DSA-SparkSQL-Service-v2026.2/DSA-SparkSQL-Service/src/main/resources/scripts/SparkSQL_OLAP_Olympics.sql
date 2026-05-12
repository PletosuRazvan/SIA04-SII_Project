------ ============================================================
--- SparkSQL_OLAP_Olympics.sql
--- Integration and Analytical Model for Olympics Data
--- Run in DBeaver connected to Spark SQL (port 10000)
--- ============================================================
--- PREREQUISITES: Run these scripts first:
--- 1. DS_DOC_CSV_SparkSQL_Views.sql    (creates ATHLETES_VIEW, GAMES_VIEW)
--- 2. DS_SQL_PG_SparkSQL_Views.sql     (creates NOC_REGIONS_VIEW)
--- 3. DS_MongoDB_SparkSQL_Views.sql    (creates RESULTS_VIEW)
--- ============================================================

-- Verify access model views
SELECT * FROM ATHLETES_VIEW;
SELECT * FROM GAMES_VIEW;
SELECT * FROM NOC_REGIONS_VIEW;
SELECT * FROM RESULTS_VIEW;

--------------------------------------------------------------------------------
--- CONSOLIDATION VIEW (Integration Model)
--- Joins all data sources: Athletes (CSV) + Results (MongoDB) + Games (CSV) + Regions (PostgreSQL)
--------------------------------------------------------------------------------

-- DROP VIEW OLAP_CONSOLIDARE_OLIMPICA;
CREATE OR REPLACE VIEW OLAP_CONSOLIDARE_OLIMPICA AS
SELECT
    A.id AS Athlete_ID,
    A.name AS Athlete_Name,
    A.sex AS Sex,
    A.height AS Height,
    A.weight AS Weight,
    R.sport AS Sport,
    R.event AS Event,
    R.medal AS Medal,
    R.age AS Age,
    G.year AS Year,
    G.season AS Season,
    G.city AS City,
    R.noc AS NOC,
    N.region AS Country_Name
FROM ATHLETES_VIEW A
    INNER JOIN RESULTS_VIEW R ON A.id = R.athleteId
    INNER JOIN GAMES_VIEW G ON R.games = G.games
    LEFT JOIN NOC_REGIONS_VIEW N ON R.noc = N.noc;

SELECT * FROM OLAP_CONSOLIDARE_OLIMPICA;

--------------------------------------------------------------------------------
--- OLAP DIMENSIONS
--------------------------------------------------------------------------------

--- D1: Athlete Dimension
-- DROP VIEW OLAP_DIM_ATHLETES;
CREATE OR REPLACE VIEW OLAP_DIM_ATHLETES AS
SELECT DISTINCT
    Athlete_ID,
    Athlete_Name,
    Sex,
    Height,
    Weight,
    Country_Name
FROM OLAP_CONSOLIDARE_OLIMPICA;

SELECT * FROM OLAP_DIM_ATHLETES;

--- D2: Time Dimension (Games/Years)
-- DROP VIEW OLAP_DIM_TIME;
CREATE OR REPLACE VIEW OLAP_DIM_TIME AS
SELECT DISTINCT
    Year,
    Season,
    City
FROM OLAP_CONSOLIDARE_OLIMPICA
ORDER BY Year;

SELECT * FROM OLAP_DIM_TIME;

--- D3: Sport Dimension
-- DROP VIEW OLAP_DIM_SPORT;
CREATE OR REPLACE VIEW OLAP_DIM_SPORT AS
SELECT DISTINCT
    Sport,
    Event
FROM OLAP_CONSOLIDARE_OLIMPICA
ORDER BY Sport, Event;

SELECT * FROM OLAP_DIM_SPORT;

--- D4: Country/Region Dimension
-- DROP VIEW OLAP_DIM_COUNTRY;
CREATE OR REPLACE VIEW OLAP_DIM_COUNTRY AS
SELECT DISTINCT
    NOC,
    Country_Name
FROM OLAP_CONSOLIDARE_OLIMPICA
WHERE Country_Name IS NOT NULL
ORDER BY Country_Name;

SELECT * FROM OLAP_DIM_COUNTRY;

--------------------------------------------------------------------------------
--- OLAP FACTS
--------------------------------------------------------------------------------

--- Facts: Medal Count per Athlete, Sport, Year, Country
-- DROP VIEW OLAP_FACTS_MEDALS;
CREATE OR REPLACE VIEW OLAP_FACTS_MEDALS AS
SELECT
    Athlete_ID,
    Sport,
    Year,
    NOC,
    Country_Name,
    COUNT(*) AS Total_Participations,
    SUM(CASE WHEN Medal = 'Gold' THEN 1 ELSE 0 END) AS Gold_Medals,
    SUM(CASE WHEN Medal = 'Silver' THEN 1 ELSE 0 END) AS Silver_Medals,
    SUM(CASE WHEN Medal = 'Bronze' THEN 1 ELSE 0 END) AS Bronze_Medals,
    SUM(CASE WHEN Medal <> 'NA' THEN 1 ELSE 0 END) AS Total_Medals
FROM OLAP_CONSOLIDARE_OLIMPICA
GROUP BY Athlete_ID, Sport, Year, NOC, Country_Name;

SELECT * FROM OLAP_FACTS_MEDALS;

--------------------------------------------------------------------------------
--- ANALYTICAL VIEWS (Multidimensional)
--------------------------------------------------------------------------------

--- A1: CUBE - Country x Sport Medal Analysis
-- DROP VIEW OLAP_VIEW_CUBE_COUNTRY_SPORT;
CREATE OR REPLACE VIEW OLAP_VIEW_CUBE_COUNTRY_SPORT AS
SELECT
    NVL(Country_Name, '{ALL COUNTRIES}') AS Country_Name,
    NVL(Sport, '{ALL SPORTS}') AS Sport,
    SUM(Total_Medals) AS Nr_Medals
FROM OLAP_FACTS_MEDALS
WHERE Total_Medals > 0
GROUP BY CUBE(Country_Name, Sport)
ORDER BY 1, 2;

SELECT * FROM OLAP_VIEW_CUBE_COUNTRY_SPORT;

--- A2: ROLLUP - Time Hierarchy: Year > Season > City
-- DROP VIEW OLAP_VIEW_ROLLUP_TIME;
CREATE OR REPLACE VIEW OLAP_VIEW_ROLLUP_TIME AS
SELECT
    CASE
        WHEN Year IS NULL THEN '{Total General}'
        ELSE CAST(Year AS STRING) END AS Year,
    CASE
        WHEN Year IS NULL THEN ' '
        WHEN Season IS NULL THEN 'subtotal year'
        ELSE Season END AS Season,
    CASE
        WHEN Year IS NULL THEN ' '
        WHEN Season IS NULL THEN ' '
        WHEN City IS NULL THEN 'subtotal season'
        ELSE City END AS City,
    COUNT(DISTINCT Athlete_ID) AS Nr_Participants,
    SUM(CASE WHEN Medal <> 'NA' THEN 1 ELSE 0 END) AS Nr_Medals
FROM OLAP_CONSOLIDARE_OLIMPICA
GROUP BY ROLLUP(Year, Season, City)
ORDER BY 1, 2, 3;

SELECT * FROM OLAP_VIEW_ROLLUP_TIME;

--- A3: GROUPING SETS - Distribution by Sex and Medal
-- DROP VIEW OLAP_VIEW_GROUPING_SETS;
CREATE OR REPLACE VIEW OLAP_VIEW_GROUPING_SETS AS
SELECT
    Sex,
    Medal,
    COUNT(*) AS Total
FROM OLAP_CONSOLIDARE_OLIMPICA
WHERE Medal <> 'NA'
GROUP BY GROUPING SETS(Sex, Medal);

SELECT * FROM OLAP_VIEW_GROUPING_SETS;

--- A4: Ranking - Top Athletes by Medal Count per Sport
-- DROP VIEW OLAP_VIEW_RANK_ATHLETES;
CREATE OR REPLACE VIEW OLAP_VIEW_RANK_ATHLETES AS
SELECT
    Athlete_Name,
    Sport,
    COUNT(*) AS Medals,
    DENSE_RANK() OVER (PARTITION BY Sport ORDER BY COUNT(*) DESC) AS Rank_In_Sport
FROM OLAP_CONSOLIDARE_OLIMPICA
WHERE Medal <> 'NA'
GROUP BY Athlete_Name, Sport;

SELECT * FROM OLAP_VIEW_RANK_ATHLETES;

--- A5: Window Function - Country Participation Evolution
-- DROP VIEW OLAP_VIEW_EVOLUTION;
CREATE OR REPLACE VIEW OLAP_VIEW_EVOLUTION AS
SELECT
    Country_Name,
    Year,
    COUNT(DISTINCT Athlete_ID) AS Athletes_Present,
    LAG(COUNT(DISTINCT Athlete_ID)) OVER (PARTITION BY Country_Name ORDER BY Year) AS Athletes_Previous_Edition
FROM OLAP_CONSOLIDARE_OLIMPICA
GROUP BY Country_Name, Year;

SELECT * FROM OLAP_VIEW_EVOLUTION;

--- A6: ROLLUP - Country > Sport > Athlete Medal Hierarchy
-- DROP VIEW OLAP_VIEW_COUNTRY_SPORT_ATHLETE;
CREATE OR REPLACE VIEW OLAP_VIEW_COUNTRY_SPORT_ATHLETE AS
SELECT
    CASE
        WHEN Country_Name IS NULL THEN '{Total General}'
        ELSE Country_Name END AS Country_Name,
    CASE
        WHEN Country_Name IS NULL THEN ' '
        WHEN Sport IS NULL THEN 'subtotal ' || Country_Name
        ELSE Sport END AS Sport,
    CASE
        WHEN Country_Name IS NULL THEN ' '
        WHEN Sport IS NULL THEN ' '
        WHEN Athlete_Name IS NULL THEN 'subtotal ' || Sport
        ELSE Athlete_Name END AS Athlete_Name,
    SUM(CASE WHEN Medal <> 'NA' THEN 1 ELSE 0 END) AS Total_Medals
FROM OLAP_CONSOLIDARE_OLIMPICA
GROUP BY ROLLUP(Country_Name, Sport, Athlete_Name)
ORDER BY 1, 2, 3;

SELECT * FROM OLAP_VIEW_COUNTRY_SPORT_ATHLETE;

--- A7: Statistical Analysis - Height by Sport
SELECT
    Sport,
    ROUND(AVG(Height), 2) AS Avg_Height,
    ROUND(STDDEV(Height), 2) AS Stddev_Height,
    MIN(Height) AS Min_Height,
    MAX(Height) AS Max_Height,
    COUNT(DISTINCT Athlete_ID) AS Nr_Athletes
FROM OLAP_CONSOLIDARE_OLIMPICA
WHERE Height IS NOT NULL
GROUP BY Sport
ORDER BY Avg_Height DESC;

--- A8: Pearson Correlation - Age vs Medal Success
WITH athlete_stats AS (
    SELECT
        Athlete_ID,
        AVG(Age) AS avg_age,
        SUM(CASE WHEN Medal <> 'NA' THEN 1 ELSE 0 END) AS total_medals
    FROM OLAP_CONSOLIDARE_OLIMPICA
    WHERE Age IS NOT NULL
    GROUP BY Athlete_ID
)
SELECT ROUND(CORR(avg_age, total_medals), 4) AS Correlation_Age_Medals
FROM athlete_stats;

--------------------------------------------------------------------------------
--- AUTOREST: Enable REST endpoints for SparkSQL views
--------------------------------------------------------------------------------

-- Enable AUTOREST for key views
ALTER VIEW OLAP_CONSOLIDARE_OLIMPICA SET TBLPROPERTIES('AUTOREST' = 'olap/consolidare');
ALTER VIEW OLAP_VIEW_CUBE_COUNTRY_SPORT SET TBLPROPERTIES('AUTOREST' = 'olap/cube/country_sport');
ALTER VIEW OLAP_VIEW_ROLLUP_TIME SET TBLPROPERTIES('AUTOREST' = 'olap/rollup/time');
ALTER VIEW OLAP_VIEW_RANK_ATHLETES SET TBLPROPERTIES('AUTOREST' = 'olap/rank/athletes');
ALTER VIEW OLAP_VIEW_COUNTRY_SPORT_ATHLETE SET TBLPROPERTIES('AUTOREST' = 'olap/view/country_sport_athlete');
ALTER VIEW OLAP_VIEW_EVOLUTION SET TBLPROPERTIES('AUTOREST' = 'olap/view/evolution');

-- Verify all views
SHOW VIEWS;

