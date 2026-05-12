# Olympics OLAP Data Integration Project

## Prerequisites
- PostgreSQL running on port 5432 (database: `olympics`, user/pass: `postgres/postgres`)
- MongoDB running on port 27017 (database: `olympics`)
- JDK 21 at `D:\Facultate\jdk-21.0.6`
- Hadoop winutils at `D:\Facultate\hadoop`
- DBeaver with Apache Hive 2 driver

## Step 1: Start Access Model Services

Open 3 separate terminals and run:

```powershell
# Terminal 1 - XLS Service (port 8094)
cd D:\Facultate\SII\P2\1_Access_Model\DSA-DOC-XLSService\DSA-DOC-XLSService
$env:JAVA_HOME = "D:\Facultate\jdk-21.0.6"
.\mvnw.cmd spring-boot:run

# Terminal 2 - PostgreSQL JPA Service (port 8091)
cd D:\Facultate\SII\P2\1_Access_Model\DSA-SQL-JPAService\DSA-SQL-JPAService
$env:JAVA_HOME = "D:\Facultate\jdk-21.0.6"
.\mvnw.cmd spring-boot:run

# Terminal 3 - MongoDB Service (port 8093)
cd D:\Facultate\SII\P2\1_Access_Model\DSA-NoSQL-MongoDBService\DSA-NoSQL-MongoDBService
$env:JAVA_HOME = "D:\Facultate\jdk-21.0.6"
.\mvnw.cmd spring-boot:run
```

## Step 2: Start SparkSQL Integration Service

```powershell
$env:HADOOP_HOME = "D:\Facultate\hadoop"
$env:PATH = "$env:PATH;D:\Facultate\hadoop\bin"
& "D:\Facultate\jdk-21.0.6\bin\java.exe" --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/sun.util.calendar=ALL-UNNAMED "-Dhadoop.home.dir=D:\Facultate\hadoop" -jar "D:\Facultate\SII\P2\2_Integration_Model\DSA-SparkSQL-Service-v2026.2\DSA-SparkSQL-Service\target\DSA-SparkSQL-Service-2026.2.jar"
```

Verify: http://localhost:9990/DSA-SparkSQL-Service/rest/ping

## Step 3: Connect DBeaver to SparkSQL

- Driver: Apache Hive 2
- URL: `jdbc:hive2://localhost:10000/default`
- User: `spark`, Password: `sql`

## Step 4: Run SQL Scripts (one statement at a time with Ctrl+Enter)

Run in this order:
1. `scripts/DS_DOC_XLSx.sql` → creates ATHLETES_VIEW, GAMES_VIEW
2. `scripts/DS_SQL_PG.sql` → creates NOC_REGIONS_VIEW
3. `scripts/DS_MongoDb.sql` → creates RESULTS_VIEW
4. `scripts/SparkSQL_OLAP.sql` → creates all OLAP views

## Step 5: Reload AUTOREST endpoints

Open in browser: http://localhost:9990/DSA-SparkSQL-Service/rest/auto?redef=true

## REST Endpoints

| View | URL |
|------|-----|
| Consolidation | http://localhost:9990/DSA-SparkSQL-Service/rest/view/olap/consolidare |
| CUBE Country×Sport | http://localhost:9990/DSA-SparkSQL-Service/rest/view/olap/cube/country_sport |
| ROLLUP Time | http://localhost:9990/DSA-SparkSQL-Service/rest/view/olap/rollup/time |
| Rank Athletes | http://localhost:9990/DSA-SparkSQL-Service/rest/view/olap/rank/athletes |
| Country→Sport→Athlete | http://localhost:9990/DSA-SparkSQL-Service/rest/view/olap/view/country_sport_athlete |
| Evolution (LAG) | http://localhost:9990/DSA-SparkSQL-Service/rest/view/olap/view/evolution |

## Architecture

```
[XLSX/CSV] → DSA-DOC-XLSService (8094) ──┐
[PostgreSQL] → DSA-SQL-JPAService (8091) ──┼──→ DSA-SparkSQL-Service (9990/10000) → REST API
[MongoDB] → DSA-NoSQL-MongoDBService (8093)┘         ↓
                                              OLAP Views (DBeaver)
```

## Data Sources
- **Athletes** (XLSX): id, name, sex, height, weight
- **Games** (XLSX): games, year, season, city
- **NOC Regions** (PostgreSQL): noc, region, notes
- **Results** (MongoDB): Athlete_ID, Games, NOC, Sport, Event, Medal, Age

## OLAP Views Created (20 total)
- 6 Access views (JSON + exploded)
- 1 Consolidation (INNER JOIN all sources)
- 4 Dimensions (Athletes, Time, Sport, Country)
- 1 Facts (Medals aggregation)
- 6 Analytical (CUBE, ROLLUP, GROUPING SETS, RANK, LAG)
- 3 Advanced queries (Statistics, Correlation, Ranking)
