#!/usr/bin/env python3
"""Fix view-urile cu probleme: JSON paths + Data Sources aggregate."""

import oracledb

WALLET_DIR = r"C:\Users\Razvan.PLETOSU\Downloads\Wallet_OlympicsDB"
conn = oracledb.connect(
    user="ADMIN", password="Oracle_1234#", dsn="olympicsdb_low",
    config_dir=WALLET_DIR, wallet_location=WALLET_DIR, wallet_password="Oracle_1234#"
)
cursor = conn.cursor()
print(f"Conectat: {conn.version}")

sql1 = """
CREATE OR REPLACE VIEW FDBO.FDBO_JSON_ATHLETE_MEDALS_V AS
SELECT 
    j.doc_id,
    jt.athlete_name,
    jt.total_medals,
    jt.gold_medals,
    jt.silver_medals,
    jt.bronze_medals,
    jt.sports_count,
    jt.games_count,
    'MongoDB/JSON' AS data_source
FROM FDBO.JSON_MEDAL_DOCS j,
     JSON_TABLE(j.json_data, '$'
         COLUMNS (
             athlete_name  VARCHAR2(300)  PATH '$.name',
             total_medals  NUMBER(5)      PATH '$.total_medals',
             gold_medals   NUMBER(5)      PATH '$.gold',
             silver_medals NUMBER(5)      PATH '$.silver',
             bronze_medals NUMBER(5)      PATH '$.bronze',
             sports_count  NUMBER(5)      PATH '$.sports_count',
             games_count   NUMBER(5)      PATH '$.games_count'
         )
     ) jt
WHERE j.doc_type = 'athlete_medal'
"""
cursor.execute(sql1)
conn.commit()
print("1. ✅ FDBO_JSON_ATHLETE_MEDALS_V - fixed")

sql2 = """
CREATE OR REPLACE VIEW FDBO.FDBO_JSON_GAME_SUMMARY_V AS
SELECT
    j.doc_id,
    jt.games_name,
    jt.year,
    jt.season,
    jt.city,
    jt.total_athletes,
    jt.total_entries,
    jt.countries_count,
    jt.events_count,
    'MongoDB/JSON' AS data_source
FROM FDBO.JSON_MEDAL_DOCS j,
     JSON_TABLE(j.json_data, '$'
         COLUMNS (
             games_name      VARCHAR2(50)  PATH '$.games_name',
             year            NUMBER(4)     PATH '$.year',
             season          VARCHAR2(10)  PATH '$.season',
             city            VARCHAR2(100) PATH '$.city',
             total_athletes  NUMBER(10)    PATH '$.total_athletes',
             total_entries   NUMBER(10)    PATH '$.total_entries',
             countries_count NUMBER(5)     PATH '$.countries_count',
             events_count    NUMBER(5)     PATH '$.events_count'
         )
     ) jt
WHERE j.doc_type = 'game_summary'
"""
cursor.execute(sql2)
conn.commit()
print("2. ✅ FDBO_JSON_GAME_SUMMARY_V - fixed")

sql3 = """
CREATE OR REPLACE VIEW FDBO.OLAP_GS_DATA_SOURCES_V AS
SELECT 
    source_name,
    category,
    SUM(record_count) AS record_count,
    GROUPING(source_name) AS is_source_total,
    GROUPING(category)    AS is_category_total,
    GROUPING_ID(source_name, category) AS grouping_id
FROM (
    SELECT 'DS1-Oracle' AS source_name, 'Sports'    AS category, COUNT(*) AS record_count FROM OLY_REF.SPORTS
    UNION ALL
    SELECT 'DS1-Oracle', 'Events',    COUNT(*) FROM OLY_REF.EVENTS
    UNION ALL
    SELECT 'DS1-Oracle', 'Games',     COUNT(*) FROM OLY_REF.GAMES
    UNION ALL
    SELECT 'DS1-Oracle', 'Countries', COUNT(*) FROM OLY_REF.COUNTRIES
    UNION ALL
    SELECT 'DS2-PostgreSQL', 'Athletes', COUNT(*) FROM FDBO.PG_ATHLETES
    UNION ALL
    SELECT 'DS2-PostgreSQL', 'Results',  COUNT(*) FROM FDBO.PG_RESULTS
    UNION ALL
    SELECT 'DS3-JSON/MongoDB', 'Medal Docs',   COUNT(*) FROM FDBO.JSON_MEDAL_DOCS WHERE doc_type = 'athlete_medal'
    UNION ALL
    SELECT 'DS3-JSON/MongoDB', 'Game Summaries', COUNT(*) FROM FDBO.JSON_MEDAL_DOCS WHERE doc_type = 'game_summary'
    UNION ALL
    SELECT 'DS4-CSV', 'Enriched Events', COUNT(*) FROM FDBO.CSV_ATHLETE_EVENTS
) src
GROUP BY GROUPING SETS (
    (source_name, category),
    (source_name),
    ()
)
"""
cursor.execute(sql3)
conn.commit()
print("3. ✅ OLAP_GS_DATA_SOURCES_V - fixed")

print("\n--- Fixing ORDS endpoint for json/athlete-medals ---")
try:
    cursor.execute("""
    BEGIN
        ORDS.DEFINE_HANDLER(
            p_module_name    => 'olympics',
            p_pattern        => 'json/athlete-medals',
            p_method         => 'GET',
            p_source_type    => 'json/collection',
            p_source         => 'SELECT athlete_name, total_medals, gold_medals, silver_medals, bronze_medals, sports_count, games_count, data_source FROM FDBO.FDBO_JSON_ATHLETE_MEDALS_V ORDER BY total_medals DESC FETCH FIRST 50 ROWS ONLY'
        );
        COMMIT;
    END;
    """)
    conn.commit()
    print("4. ✅ ORDS /json/athlete-medals handler updated")
except Exception as e:
    print(f"4. ⚠️ ORDS update: {e}")

print("\n" + "="*60)
print("🔍 TESTE")
print("="*60)

print("\n--- JSON Athlete Medals (top 5) ---")
cursor.execute("""
    SELECT athlete_name, total_medals, gold_medals, silver_medals, bronze_medals 
    FROM FDBO.FDBO_JSON_ATHLETE_MEDALS_V 
    ORDER BY total_medals DESC 
    FETCH FIRST 5 ROWS ONLY
""")
for r in cursor.fetchall():
    print(f"   🏅 {r[0]}: {r[1]} medals (🥇{r[2]} 🥈{r[3]} 🥉{r[4]})")

print("\n--- JSON Game Summary (first 5 editions) ---")
cursor.execute("""
    SELECT games_name, year, total_athletes, countries_count, events_count 
    FROM FDBO.FDBO_JSON_GAME_SUMMARY_V 
    ORDER BY year 
    FETCH FIRST 5 ROWS ONLY
""")
for r in cursor.fetchall():
    print(f"   🏟️  {r[0]} ({r[1]}): {r[2]} athletes, {r[3]} countries, {r[4]} events")

print("\n--- Data Sources Overview (GROUPING SETS) ---")
cursor.execute("""
    SELECT source_name, category, record_count, grouping_id 
    FROM FDBO.OLAP_GS_DATA_SOURCES_V 
    ORDER BY grouping_id, source_name
""")
for r in cursor.fetchall():
    src = r[0] if r[0] else '*** GRAND TOTAL ***'
    cat = r[1] if r[1] else '(subtotal)'
    print(f"   📊 [{r[3]}] {src} / {cat}: {r[2]:,}")

cursor.execute("SELECT COUNT(*) FROM all_views WHERE owner = 'FDBO'")
total = cursor.fetchone()[0]
print(f"\n✅ Total views in FDBO: {total}")

cursor.close()
conn.close()
print("\n🏁 All fixes applied!")
