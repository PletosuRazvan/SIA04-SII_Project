#!/usr/bin/env python3
"""
Crează toate VIEW-urile și configurează ORDS pe Oracle Cloud.
Execută: CLOUD_06, CLOUD_07, CLOUD_08, CLOUD_09, CLOUD_10
"""

import oracledb
import os
import re
import time

WALLET_DIR = r"C:\Users\Razvan.PLETOSU\Downloads\Wallet_OlympicsDB"
DSN = "olympicsdb_low"
ADMIN_USER = "ADMIN"
ADMIN_PASS = "Oracle_1234#"

def get_connection():
    """Conectare la Oracle Cloud ca ADMIN (thin mode)."""
    conn = oracledb.connect(
        user=ADMIN_USER,
        password=ADMIN_PASS,
        dsn=DSN,
        config_dir=WALLET_DIR,
        wallet_location=WALLET_DIR,
        wallet_password=ADMIN_PASS
    )
    print(f"✅ Conectat la Oracle Cloud: {conn.version}")
    return conn

def parse_sql_file(filepath):
    """
    Parsează un fișier SQL și extrage statement-urile individuale.
    Tratează blocuri PL/SQL (BEGIN...END;/) și DDL (CREATE ... ;).
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    statements = []
    
    plsql_pattern = re.compile(
        r'(BEGIN\s.*?END\s*;\s*)', 
        re.DOTALL | re.IGNORECASE
    )
    
    segments = re.split(r'\n\s*/\s*\n', content)
    
    for segment in segments:
        segment = segment.strip()
        if not segment:
            continue
            
        plsql_blocks = list(plsql_pattern.finditer(segment))
        
        if plsql_blocks:
            for match in plsql_blocks:
                block = match.group(1).strip()
                if block:
                    statements.append(('PLSQL', block))
            
            remaining = segment
            for match in plsql_blocks:
                remaining = remaining.replace(match.group(0), '')
            
            ddl_stmts = parse_ddl_statements(remaining)
            statements.extend(ddl_stmts)
        else:
            ddl_stmts = parse_ddl_statements(segment)
            statements.extend(ddl_stmts)
    
    return statements

def parse_ddl_statements(text):
    """Parsează statement-uri DDL (CREATE VIEW, SELECT) dintr-un text."""
    statements = []
    text = text.strip()
    if not text:
        return statements
    
    lines = text.split('\n')
    clean_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('--'):
            continue
        clean_lines.append(line)
    
    text = '\n'.join(clean_lines).strip()
    if not text:
        return statements
    
    parts = text.split(';')
    
    for part in parts:
        part = part.strip()
        if not part:
            continue
        clean = re.sub(r'--.*$', '', part, flags=re.MULTILINE).strip()
        if not clean:
            continue
        first_word = clean.split()[0].upper() if clean.split() else ''
        if first_word in ('CREATE', 'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 
                          'ALTER', 'GRANT', 'REVOKE', 'WITH'):
            statements.append(('DDL', part.strip()))
    
    return statements

def execute_sql_file(conn, filepath, description):
    """Execută toate statement-urile din fișierul SQL."""
    print(f"\n{'='*60}")
    print(f"📋 {description}")
    print(f"   Fișier: {os.path.basename(filepath)}")
    print(f"{'='*60}")
    
    statements = parse_sql_file(filepath)
    success = 0
    errors = 0
    
    cursor = conn.cursor()
    
    for i, (stmt_type, stmt) in enumerate(statements, 1):
        first_line = stmt.strip().split('\n')[0][:80]
        
        try:
            if stmt_type == 'PLSQL':
                cursor.execute(stmt)
            else:
                cursor.execute(stmt)
            
            if stmt.strip().upper().startswith('SELECT'):
                rows = cursor.fetchmany(5)
                if rows:
                    print(f"   ✅ [{i}] {first_line}")
                    cols = [d[0] for d in cursor.description]
                    print(f"       Coloane: {', '.join(cols[:5])}{'...' if len(cols)>5 else ''}")
                    print(f"       Primele rânduri: {len(rows)}")
                else:
                    print(f"   ✅ [{i}] {first_line} (0 rows)")
            else:
                print(f"   ✅ [{i}] {first_line}")
            
            success += 1
            
        except oracledb.DatabaseError as e:
            error_msg = str(e).split('\n')[0][:100]
            if 'ORA-00955' in str(e):  # name already used
                print(f"   ⚠️  [{i}] {first_line} (deja există, skip)")
                success += 1
            elif 'ORA-00942' in str(e):  # table/view does not exist (in SELECT tests)
                print(f"   ⚠️  [{i}] {first_line} (obiect inexistent)")
                errors += 1
            else:
                print(f"   ❌ [{i}] {first_line}")
                print(f"       Eroare: {error_msg}")
                errors += 1
    
    conn.commit()
    cursor.close()
    
    print(f"\n   📊 Rezultat: {success} OK, {errors} erori din {len(statements)} total")
    return success, errors

def execute_views_directly(conn):
    """
    Execută view-urile direct din Python (mai sigur decât parsarea SQL).
    Fiecare view este un statement separat.
    """
    cursor = conn.cursor()
    
    print(f"\n{'='*60}")
    print(f"📋 STEP 1: Access Views (CLOUD_06)")
    print(f"{'='*60}")
    
    access_views = [
        ("FDBO.FDBO_SPORTS_V", """
CREATE OR REPLACE VIEW FDBO.FDBO_SPORTS_V AS
SELECT sport_id, sport_name, 'Oracle/OLY_REF' AS data_source
FROM OLY_REF.SPORTS
        """),
        ("FDBO.FDBO_EVENTS_V", """
CREATE OR REPLACE VIEW FDBO.FDBO_EVENTS_V AS
SELECT e.event_id, e.sport_id, e.event_name, 
       s.sport_name,
       'Oracle/OLY_REF' AS data_source
FROM OLY_REF.EVENTS e
JOIN OLY_REF.SPORTS s ON e.sport_id = s.sport_id
        """),
        ("FDBO.FDBO_GAMES_V", """
CREATE OR REPLACE VIEW FDBO.FDBO_GAMES_V AS
SELECT game_id, games_name, year, season, city,
       'Oracle/OLY_REF' AS data_source
FROM OLY_REF.GAMES
        """),
        ("FDBO.FDBO_COUNTRIES_V", """
CREATE OR REPLACE VIEW FDBO.FDBO_COUNTRIES_V AS
SELECT noc, region, notes,
       'Oracle/OLY_REF' AS data_source
FROM OLY_REF.COUNTRIES
        """),
        ("FDBO.FDBO_PG_ATHLETES_V", """
CREATE OR REPLACE VIEW FDBO.FDBO_PG_ATHLETES_V AS
SELECT athlete_id, full_name, sex, birth_year, birth_place,
       height_cm, weight_kg, noc,
       'PostgreSQL' AS data_source
FROM FDBO.PG_ATHLETES
        """),
        ("FDBO.FDBO_PG_RESULTS_V", """
CREATE OR REPLACE VIEW FDBO.FDBO_PG_RESULTS_V AS
SELECT result_id, athlete_id, game_id, event_id,
       medal, age, team, noc,
       'PostgreSQL' AS data_source
FROM FDBO.PG_RESULTS
        """),
        ("FDBO.FDBO_JSON_ATHLETE_MEDALS_V", """
CREATE OR REPLACE VIEW FDBO.FDBO_JSON_ATHLETE_MEDALS_V AS
SELECT 
    j.doc_id,
    jt.athlete_name,
    jt.noc,
    jt.total_medals,
    jt.gold_medals,
    jt.silver_medals,
    jt.bronze_medals,
    jt.sports_list,
    jt.first_year,
    jt.last_year,
    'MongoDB/JSON' AS data_source
FROM FDBO.JSON_MEDAL_DOCS j,
     JSON_TABLE(j.json_data, '$'
         COLUMNS (
             athlete_name  VARCHAR2(300)  PATH '$.athlete_name',
             noc           VARCHAR2(10)   PATH '$.noc',
             total_medals  NUMBER(5)      PATH '$.total_medals',
             gold_medals   NUMBER(5)      PATH '$.gold_count',
             silver_medals NUMBER(5)      PATH '$.silver_count',
             bronze_medals NUMBER(5)      PATH '$.bronze_count',
             sports_list   VARCHAR2(1000) PATH '$.sports',
             first_year    NUMBER(4)      PATH '$.first_year',
             last_year     NUMBER(4)      PATH '$.last_year'
         )
     ) jt
WHERE j.doc_type = 'athlete_medal'
        """),
        ("FDBO.FDBO_JSON_GAME_SUMMARY_V", """
CREATE OR REPLACE VIEW FDBO.FDBO_JSON_GAME_SUMMARY_V AS
SELECT
    j.doc_id,
    jt.games_name,
    jt.year,
    jt.season,
    jt.city,
    jt.total_athletes,
    jt.total_countries,
    jt.total_events,
    jt.total_medals,
    'MongoDB/JSON' AS data_source
FROM FDBO.JSON_MEDAL_DOCS j,
     JSON_TABLE(j.json_data, '$'
         COLUMNS (
             games_name      VARCHAR2(50)  PATH '$.games',
             year            NUMBER(4)     PATH '$.year',
             season          VARCHAR2(10)  PATH '$.season',
             city            VARCHAR2(100) PATH '$.city',
             total_athletes  NUMBER(10)    PATH '$.total_athletes',
             total_countries NUMBER(5)     PATH '$.total_countries',
             total_events    NUMBER(5)     PATH '$.total_events',
             total_medals    NUMBER(10)    PATH '$.total_medals'
         )
     ) jt
WHERE j.doc_type = 'game_summary'
        """),
        ("FDBO.FDBO_CSV_EVENTS_V", """
CREATE OR REPLACE VIEW FDBO.FDBO_CSV_EVENTS_V AS
SELECT row_id, athlete_name, sex, age, height, weight,
       team, noc, games, year, season, city,
       sport, event, medal, region,
       'CSV' AS data_source
FROM FDBO.CSV_ATHLETE_EVENTS
        """),
    ]
    
    exec_views(cursor, conn, access_views)
    
    print(f"\n{'='*60}")
    print(f"📋 STEP 2: Integration Views (CLOUD_07)")
    print(f"   VIEW URIAȘ + Dimensiuni + Fapte")
    print(f"{'='*60}")
    
    integration_views = [
        ("FDBO.INT_RESULTS_FULL_V (VIEW URIAȘ)", """
CREATE OR REPLACE VIEW FDBO.INT_RESULTS_FULL_V AS
SELECT 
    r.result_id,
    r.athlete_id,
    a.full_name       AS athlete_name,
    a.sex,
    r.age,
    a.height_cm,
    a.weight_kg,
    r.team,
    r.medal,
    g.game_id,
    g.games_name,
    g.year            AS game_year,
    g.season,
    g.city,
    e.event_id,
    e.event_name,
    s.sport_id,
    s.sport_name,
    c.noc,
    c.region           AS country_name,
    c.notes            AS country_notes,
    'PG+Oracle' AS source_type
FROM FDBO.PG_RESULTS r
JOIN FDBO.PG_ATHLETES a   ON r.athlete_id = a.athlete_id
LEFT JOIN OLY_REF.GAMES g     ON r.game_id    = g.game_id
LEFT JOIN OLY_REF.EVENTS e    ON r.event_id   = e.event_id
LEFT JOIN OLY_REF.SPORTS s    ON e.sport_id   = s.sport_id
LEFT JOIN OLY_REF.COUNTRIES c ON r.noc         = c.noc
        """),
        ("FDBO.DIM_ATHLETE_V", """
CREATE OR REPLACE VIEW FDBO.DIM_ATHLETE_V AS
SELECT DISTINCT
    a.athlete_id,
    a.full_name,
    a.sex,
    a.birth_year,
    a.height_cm,
    a.weight_kg,
    a.noc,
    c.region AS country_name
FROM FDBO.PG_ATHLETES a
LEFT JOIN OLY_REF.COUNTRIES c ON a.noc = c.noc
        """),
        ("FDBO.DIM_GAME_V", """
CREATE OR REPLACE VIEW FDBO.DIM_GAME_V AS
SELECT 
    g.game_id,
    g.games_name,
    g.year,
    g.season,
    g.city,
    CASE 
        WHEN g.year < 1920 THEN 'Pionier (pre-1920)'
        WHEN g.year < 1950 THEN 'Interbelic (1920-1948)'
        WHEN g.year < 1980 THEN 'Epoca de Aur (1952-1976)'
        WHEN g.year < 2000 THEN 'Modern (1980-1996)'
        ELSE 'Contemporan (2000+)'
    END AS era
FROM OLY_REF.GAMES g
        """),
        ("FDBO.DIM_EVENT_V", """
CREATE OR REPLACE VIEW FDBO.DIM_EVENT_V AS
SELECT 
    e.event_id,
    e.event_name,
    s.sport_id,
    s.sport_name,
    CASE 
        WHEN INSTR(e.event_name, 'Men''s') > 0 THEN 'Men'
        WHEN INSTR(e.event_name, 'Women''s') > 0 THEN 'Women'
        ELSE 'Mixed'
    END AS gender_category
FROM OLY_REF.EVENTS e
JOIN OLY_REF.SPORTS s ON e.sport_id = s.sport_id
        """),
        ("FDBO.DIM_COUNTRY_V", """
CREATE OR REPLACE VIEW FDBO.DIM_COUNTRY_V AS
SELECT 
    c.noc,
    c.region AS country_name,
    c.notes,
    CASE 
        WHEN c.noc IN ('USA','CAN','MEX','GUA','CUB','JAM','PUR','HAI','DOM','TTO','BAH','BAR','BIZ','CRC','ESA','GRN','HON','NCA','PAN','VIN','ANT','ARU','BER','CAY','DMA','ISV','IVB','LCA','SKN') THEN 'Americas - North'
        WHEN c.noc IN ('ARG','BOL','BRA','CHI','COL','ECU','GUY','PAR','PER','SUR','URU','VEN') THEN 'Americas - South'
        WHEN c.noc IN ('GBR','FRA','GER','ITA','ESP','POR','NED','BEL','SUI','AUT','SWE','NOR','DEN','FIN','ISL','IRL','LUX','MON','LIE','AND','SMR','MLT','GRE','CYP','CZE','SVK','POL','HUN','ROU','BUL','CRO','SRB','SLO','BIH','MNE','MKD','ALB','EST','LAT','LTU','BLR','UKR','MDA','GEO','ARM','AZE','RUS','FRG','GDR','TCH','EUN','YUG','BOH','SCG','URS','SAA','CRT') THEN 'Europe'
        WHEN c.noc IN ('CHN','JPN','KOR','PRK','MGL','TPE','HKG') THEN 'Asia - East'
        WHEN c.noc IN ('IND','PAK','BAN','SRI','NEP','BHU','MDV') THEN 'Asia - South'
        WHEN c.noc IN ('AUS','NZL','FIJ','SAM','TGA','PNG','SOL','VAN','FSM','KIR','MHL','NRU','PLW','COK','ASA','GUM','TUV','ANZ','NBO','NFL') THEN 'Oceania'
        WHEN c.noc IN ('EGY','RSA','NGR','KEN','ETH','GHA','CMR','SEN','CIV','TAN','UGA','ZIM','MAR','TUN','ALG','MOZ','NAM','BOT','MAW','ZAM','RWA','BDI','BEN','BUR','CAF','CHA','CGO','COD','COM','CPV','DJI','GEQ','ERI','GAB','GAM','GBS','GUI','LBA','LBR','LES','MAD','MLI','MRI','MTN','MYA','NIG','SLE','SOM','SSD','STP','SUD','SWZ','SEY','TOG','RHO') THEN 'Africa'
        ELSE 'Other'
    END AS continent
FROM OLY_REF.COUNTRIES c
        """),
        ("FDBO.FACT_RESULTS_V", """
CREATE OR REPLACE VIEW FDBO.FACT_RESULTS_V AS
SELECT 
    r.result_id,
    r.athlete_id,
    r.game_id,
    r.event_id,
    r.noc,
    r.medal,
    r.age,
    r.team,
    CASE WHEN r.medal = 'Gold'   THEN 1 ELSE 0 END AS is_gold,
    CASE WHEN r.medal = 'Silver' THEN 1 ELSE 0 END AS is_silver,
    CASE WHEN r.medal = 'Bronze' THEN 1 ELSE 0 END AS is_bronze,
    CASE WHEN r.medal IS NOT NULL THEN 1 ELSE 0 END AS has_medal,
    g.year AS game_year,
    g.season
FROM FDBO.PG_RESULTS r
LEFT JOIN OLY_REF.GAMES g ON r.game_id = g.game_id
        """),
    ]
    
    exec_views(cursor, conn, integration_views)
    
    print(f"\n{'='*60}")
    print(f"📋 STEP 3: OLAP Views (CLOUD_08)")
    print(f"   3 ROLLUP + 3 CUBE + 3 GROUPING SETS")
    print(f"{'='*60}")
    
    olap_views = [
        ("FDBO.OLAP_ROLLUP_COUNTRY_SPORT_V", """
CREATE OR REPLACE VIEW FDBO.OLAP_ROLLUP_COUNTRY_SPORT_V AS
SELECT 
    c.region                                  AS country_name,
    s.sport_name,
    COUNT(*)                                  AS participations,
    SUM(CASE WHEN r.medal IS NOT NULL THEN 1 ELSE 0 END) AS total_medals,
    SUM(CASE WHEN r.medal = 'Gold'   THEN 1 ELSE 0 END)  AS gold,
    SUM(CASE WHEN r.medal = 'Silver' THEN 1 ELSE 0 END)  AS silver,
    SUM(CASE WHEN r.medal = 'Bronze' THEN 1 ELSE 0 END)  AS bronze,
    GROUPING(c.region)                        AS is_country_total,
    GROUPING(s.sport_name)                    AS is_sport_total
FROM FDBO.PG_RESULTS r
JOIN OLY_REF.COUNTRIES c ON r.noc = c.noc
JOIN OLY_REF.EVENTS e    ON r.event_id = e.event_id
JOIN OLY_REF.SPORTS s    ON e.sport_id = s.sport_id
WHERE r.medal IS NOT NULL
GROUP BY ROLLUP(c.region, s.sport_name)
        """),
        ("FDBO.OLAP_ROLLUP_SEASON_YEAR_V", """
CREATE OR REPLACE VIEW FDBO.OLAP_ROLLUP_SEASON_YEAR_V AS
SELECT 
    g.season,
    g.year,
    g.city,
    COUNT(DISTINCT r.athlete_id)              AS unique_athletes,
    COUNT(*)                                  AS participations,
    SUM(CASE WHEN r.medal IS NOT NULL THEN 1 ELSE 0 END) AS total_medals,
    GROUPING(g.season)                        AS is_season_total,
    GROUPING(g.year)                          AS is_year_total,
    GROUPING(g.city)                          AS is_city_total
FROM FDBO.PG_RESULTS r
JOIN OLY_REF.GAMES g ON r.game_id = g.game_id
GROUP BY ROLLUP(g.season, g.year, g.city)
        """),
        ("FDBO.OLAP_ROLLUP_SEX_AGE_V", """
CREATE OR REPLACE VIEW FDBO.OLAP_ROLLUP_SEX_AGE_V AS
SELECT 
    a.sex,
    CASE 
        WHEN r.age < 18 THEN 'Junior (<18)'
        WHEN r.age BETWEEN 18 AND 25 THEN 'Tanar (18-25)'
        WHEN r.age BETWEEN 26 AND 35 THEN 'Matur (26-35)'
        WHEN r.age > 35 THEN 'Veteran (>35)'
        ELSE 'Necunoscut'
    END AS age_group,
    COUNT(*)                                  AS participations,
    SUM(CASE WHEN r.medal IS NOT NULL THEN 1 ELSE 0 END) AS total_medals,
    SUM(CASE WHEN r.medal = 'Gold'   THEN 1 ELSE 0 END)  AS gold,
    ROUND(AVG(r.age), 1)                      AS avg_age,
    GROUPING(a.sex)                           AS is_sex_total,
    GROUPING(CASE 
        WHEN r.age < 18 THEN 'Junior (<18)'
        WHEN r.age BETWEEN 18 AND 25 THEN 'Tanar (18-25)'
        WHEN r.age BETWEEN 26 AND 35 THEN 'Matur (26-35)'
        WHEN r.age > 35 THEN 'Veteran (>35)'
        ELSE 'Necunoscut'
    END)                                      AS is_age_total
FROM FDBO.PG_RESULTS r
JOIN FDBO.PG_ATHLETES a ON r.athlete_id = a.athlete_id
GROUP BY ROLLUP(a.sex, 
    CASE 
        WHEN r.age < 18 THEN 'Junior (<18)'
        WHEN r.age BETWEEN 18 AND 25 THEN 'Tanar (18-25)'
        WHEN r.age BETWEEN 26 AND 35 THEN 'Matur (26-35)'
        WHEN r.age > 35 THEN 'Veteran (>35)'
        ELSE 'Necunoscut'
    END)
        """),
        ("FDBO.OLAP_CUBE_COUNTRY_SEASON_V", """
CREATE OR REPLACE VIEW FDBO.OLAP_CUBE_COUNTRY_SEASON_V AS
SELECT 
    c.region                                  AS country_name,
    g.season,
    COUNT(DISTINCT r.athlete_id)              AS unique_athletes,
    SUM(CASE WHEN r.medal IS NOT NULL THEN 1 ELSE 0 END) AS total_medals,
    SUM(CASE WHEN r.medal = 'Gold'   THEN 1 ELSE 0 END)  AS gold,
    SUM(CASE WHEN r.medal = 'Silver' THEN 1 ELSE 0 END)  AS silver,
    SUM(CASE WHEN r.medal = 'Bronze' THEN 1 ELSE 0 END)  AS bronze,
    GROUPING(c.region)                        AS is_country_total,
    GROUPING(g.season)                        AS is_season_total
FROM FDBO.PG_RESULTS r
JOIN OLY_REF.COUNTRIES c ON r.noc = c.noc
JOIN OLY_REF.GAMES g     ON r.game_id = g.game_id
WHERE r.medal IS NOT NULL
GROUP BY CUBE(c.region, g.season)
        """),
        ("FDBO.OLAP_CUBE_SEX_MEDAL_V", """
CREATE OR REPLACE VIEW FDBO.OLAP_CUBE_SEX_MEDAL_V AS
SELECT 
    a.sex,
    r.medal                                   AS medal_type,
    COUNT(*)                                  AS medal_count,
    COUNT(DISTINCT r.athlete_id)              AS unique_medalists,
    COUNT(DISTINCT r.noc)                     AS countries_count,
    GROUPING(a.sex)                           AS is_sex_total,
    GROUPING(r.medal)                         AS is_medal_total
FROM FDBO.PG_RESULTS r
JOIN FDBO.PG_ATHLETES a ON r.athlete_id = a.athlete_id
WHERE r.medal IS NOT NULL
GROUP BY CUBE(a.sex, r.medal)
        """),
        ("FDBO.OLAP_CUBE_SPORT_ERA_V", """
CREATE OR REPLACE VIEW FDBO.OLAP_CUBE_SPORT_ERA_V AS
SELECT 
    s.sport_name,
    CASE 
        WHEN g.year < 1950 THEN 'Pre-1950'
        WHEN g.year < 1980 THEN '1950-1979'
        WHEN g.year < 2000 THEN '1980-1999'
        ELSE '2000+'
    END AS era,
    COUNT(*)                                  AS participations,
    SUM(CASE WHEN r.medal IS NOT NULL THEN 1 ELSE 0 END) AS medals,
    COUNT(DISTINCT r.noc)                     AS countries,
    GROUPING(s.sport_name)                    AS is_sport_total,
    GROUPING(CASE 
        WHEN g.year < 1950 THEN 'Pre-1950'
        WHEN g.year < 1980 THEN '1950-1979'
        WHEN g.year < 2000 THEN '1980-1999'
        ELSE '2000+'
    END)                                      AS is_era_total
FROM FDBO.PG_RESULTS r
JOIN OLY_REF.GAMES g     ON r.game_id = g.game_id
JOIN OLY_REF.EVENTS e    ON r.event_id = e.event_id
JOIN OLY_REF.SPORTS s    ON e.sport_id = s.sport_id
GROUP BY CUBE(s.sport_name, 
    CASE 
        WHEN g.year < 1950 THEN 'Pre-1950'
        WHEN g.year < 1980 THEN '1950-1979'
        WHEN g.year < 2000 THEN '1980-1999'
        ELSE '2000+'
    END)
        """),
        ("FDBO.OLAP_GS_COUNTRY_SPORT_SEASON_V", """
CREATE OR REPLACE VIEW FDBO.OLAP_GS_COUNTRY_SPORT_SEASON_V AS
SELECT 
    c.region                                  AS country_name,
    s.sport_name,
    g.season,
    COUNT(*)                                  AS participations,
    SUM(CASE WHEN r.medal IS NOT NULL THEN 1 ELSE 0 END) AS medals,
    GROUPING(c.region)                        AS is_country_grouped,
    GROUPING(s.sport_name)                    AS is_sport_grouped,
    GROUPING(g.season)                        AS is_season_grouped,
    GROUPING_ID(c.region, s.sport_name, g.season) AS grouping_id
FROM FDBO.PG_RESULTS r
JOIN OLY_REF.COUNTRIES c ON r.noc = c.noc
JOIN OLY_REF.GAMES g     ON r.game_id = g.game_id
JOIN OLY_REF.EVENTS e    ON r.event_id = e.event_id
JOIN OLY_REF.SPORTS s    ON e.sport_id = s.sport_id
WHERE r.medal IS NOT NULL
GROUP BY GROUPING SETS (
    (c.region),
    (s.sport_name),
    (g.season),
    ()
)
        """),
        ("FDBO.OLAP_GS_MIXED_V", """
CREATE OR REPLACE VIEW FDBO.OLAP_GS_MIXED_V AS
SELECT 
    c.region                                  AS country_name,
    g.year,
    s.sport_name,
    a.sex,
    SUM(CASE WHEN r.medal IS NOT NULL THEN 1 ELSE 0 END) AS medals,
    SUM(CASE WHEN r.medal = 'Gold'   THEN 1 ELSE 0 END)  AS gold,
    GROUPING_ID(c.region, g.year, s.sport_name, a.sex) AS grouping_id
FROM FDBO.PG_RESULTS r
JOIN FDBO.PG_ATHLETES a  ON r.athlete_id = a.athlete_id
JOIN OLY_REF.COUNTRIES c ON r.noc = c.noc
JOIN OLY_REF.GAMES g     ON r.game_id = g.game_id
JOIN OLY_REF.EVENTS e    ON r.event_id = e.event_id
JOIN OLY_REF.SPORTS s    ON e.sport_id = s.sport_id
WHERE r.medal IS NOT NULL
GROUP BY GROUPING SETS (
    (c.region, g.year),
    (s.sport_name, a.sex),
    (c.region),
    ()
)
        """),
        ("FDBO.OLAP_GS_DATA_SOURCES_V", """
CREATE OR REPLACE VIEW FDBO.OLAP_GS_DATA_SOURCES_V AS
SELECT 
    source_name,
    category,
    record_count,
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
        """),
    ]
    
    exec_views(cursor, conn, olap_views)
    
    print(f"\n{'='*60}")
    print(f"📋 STEP 4: Window Functions (CLOUD_09)")
    print(f"   8 views cu funcții ferestră")
    print(f"{'='*60}")
    
    window_views = [
        ("FDBO.WF_TOP_MEDALISTS_V", """
CREATE OR REPLACE VIEW FDBO.WF_TOP_MEDALISTS_V AS
SELECT 
    athlete_name,
    country_name,
    total_medals,
    gold,
    ROW_NUMBER() OVER (PARTITION BY country_name ORDER BY total_medals DESC, gold DESC) AS row_num,
    RANK()       OVER (PARTITION BY country_name ORDER BY total_medals DESC)            AS rank_in_country,
    DENSE_RANK() OVER (ORDER BY total_medals DESC)                                     AS global_dense_rank
FROM (
    SELECT 
        a.full_name AS athlete_name,
        c.region    AS country_name,
        COUNT(*)    AS total_medals,
        SUM(CASE WHEN r.medal = 'Gold' THEN 1 ELSE 0 END) AS gold
    FROM FDBO.PG_RESULTS r
    JOIN FDBO.PG_ATHLETES a  ON r.athlete_id = a.athlete_id
    JOIN OLY_REF.COUNTRIES c ON r.noc = c.noc
    WHERE r.medal IS NOT NULL
    GROUP BY a.full_name, c.region
    HAVING COUNT(*) >= 3
)
        """),
        ("FDBO.WF_RUNNING_MEDALS_V", """
CREATE OR REPLACE VIEW FDBO.WF_RUNNING_MEDALS_V AS
SELECT 
    g.year,
    g.season,
    medals_this_edition,
    SUM(medals_this_edition) OVER (ORDER BY g.year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_medals,
    athletes_this_edition,
    SUM(athletes_this_edition) OVER (ORDER BY g.year) AS cumulative_athletes
FROM (
    SELECT 
        r.game_id,
        COUNT(CASE WHEN r.medal IS NOT NULL THEN 1 END) AS medals_this_edition,
        COUNT(DISTINCT r.athlete_id) AS athletes_this_edition
    FROM FDBO.PG_RESULTS r
    GROUP BY r.game_id
) sub
JOIN OLY_REF.GAMES g ON sub.game_id = g.game_id
        """),
        ("FDBO.WF_MOVING_AVG_MEDALS_V", """
CREATE OR REPLACE VIEW FDBO.WF_MOVING_AVG_MEDALS_V AS
SELECT 
    g.year,
    g.season,
    g.city,
    medal_count,
    ROUND(AVG(medal_count) OVER (ORDER BY g.year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 1) AS moving_avg_3,
    ROUND(AVG(medal_count) OVER (ORDER BY g.year ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 1) AS moving_avg_5,
    MIN(medal_count) OVER (ORDER BY g.year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS min_window_3,
    MAX(medal_count) OVER (ORDER BY g.year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS max_window_3
FROM (
    SELECT 
        r.game_id,
        COUNT(CASE WHEN r.medal IS NOT NULL THEN 1 END) AS medal_count
    FROM FDBO.PG_RESULTS r
    GROUP BY r.game_id
) sub
JOIN OLY_REF.GAMES g ON sub.game_id = g.game_id
        """),
        ("FDBO.WF_LAG_LEAD_EDITIONS_V", """
CREATE OR REPLACE VIEW FDBO.WF_LAG_LEAD_EDITIONS_V AS
SELECT 
    g.year,
    g.season,
    g.city,
    total_athletes,
    LAG(total_athletes, 1) OVER (PARTITION BY g.season ORDER BY g.year)  AS prev_edition_athletes,
    LEAD(total_athletes, 1) OVER (PARTITION BY g.season ORDER BY g.year) AS next_edition_athletes,
    total_athletes - LAG(total_athletes, 1) OVER (PARTITION BY g.season ORDER BY g.year) AS growth,
    ROUND(
        (total_athletes - LAG(total_athletes, 1) OVER (PARTITION BY g.season ORDER BY g.year)) * 100.0 
        / NULLIF(LAG(total_athletes, 1) OVER (PARTITION BY g.season ORDER BY g.year), 0),
        1
    ) AS growth_pct
FROM (
    SELECT 
        r.game_id,
        COUNT(DISTINCT r.athlete_id) AS total_athletes
    FROM FDBO.PG_RESULTS r
    GROUP BY r.game_id
) sub
JOIN OLY_REF.GAMES g ON sub.game_id = g.game_id
        """),
        ("FDBO.WF_ATHLETE_QUARTILES_V", """
CREATE OR REPLACE VIEW FDBO.WF_ATHLETE_QUARTILES_V AS
SELECT 
    athlete_name,
    country_name,
    total_medals,
    gold,
    NTILE(4) OVER (ORDER BY total_medals DESC) AS quartile,
    PERCENT_RANK() OVER (ORDER BY total_medals) AS percent_rank,
    CUME_DIST() OVER (ORDER BY total_medals)    AS cume_dist
FROM (
    SELECT 
        a.full_name AS athlete_name,
        c.region    AS country_name,
        COUNT(*)    AS total_medals,
        SUM(CASE WHEN r.medal = 'Gold' THEN 1 ELSE 0 END) AS gold
    FROM FDBO.PG_RESULTS r
    JOIN FDBO.PG_ATHLETES a  ON r.athlete_id = a.athlete_id
    JOIN OLY_REF.COUNTRIES c ON r.noc = c.noc
    WHERE r.medal IS NOT NULL
    GROUP BY a.full_name, c.region
    HAVING COUNT(*) >= 2
)
        """),
        ("FDBO.WF_FIRST_LAST_MEDAL_V", """
CREATE OR REPLACE VIEW FDBO.WF_FIRST_LAST_MEDAL_V AS
SELECT DISTINCT
    s.sport_name,
    g.year,
    c.region AS country_name,
    r.medal,
    FIRST_VALUE(g.year) OVER (PARTITION BY s.sport_name ORDER BY g.year 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_medal_year,
    LAST_VALUE(g.year) OVER (PARTITION BY s.sport_name ORDER BY g.year 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_medal_year,
    FIRST_VALUE(c.region) OVER (PARTITION BY s.sport_name ORDER BY g.year
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_gold_country
FROM FDBO.PG_RESULTS r
JOIN OLY_REF.GAMES g     ON r.game_id = g.game_id
JOIN OLY_REF.EVENTS e    ON r.event_id = e.event_id
JOIN OLY_REF.SPORTS s    ON e.sport_id = s.sport_id
JOIN OLY_REF.COUNTRIES c ON r.noc = c.noc
WHERE r.medal = 'Gold'
        """),
        ("FDBO.WF_MEDAL_SHARE_V", """
CREATE OR REPLACE VIEW FDBO.WF_MEDAL_SHARE_V AS
SELECT 
    country_name,
    total_medals,
    gold,
    ROUND(RATIO_TO_REPORT(total_medals) OVER () * 100, 2) AS pct_of_all_medals,
    ROUND(RATIO_TO_REPORT(gold) OVER () * 100, 2)         AS pct_of_all_gold,
    RANK() OVER (ORDER BY total_medals DESC)               AS rank_total,
    RANK() OVER (ORDER BY gold DESC)                       AS rank_gold
FROM (
    SELECT 
        c.region AS country_name,
        COUNT(*) AS total_medals,
        SUM(CASE WHEN r.medal = 'Gold' THEN 1 ELSE 0 END) AS gold
    FROM FDBO.PG_RESULTS r
    JOIN OLY_REF.COUNTRIES c ON r.noc = c.noc
    WHERE r.medal IS NOT NULL
    GROUP BY c.region
)
        """),
        ("FDBO.WF_COUNTRY_TOP_SPORTS_V", """
CREATE OR REPLACE VIEW FDBO.WF_COUNTRY_TOP_SPORTS_V AS
SELECT 
    country_name,
    sport_name,
    sport_medals,
    LISTAGG(sport_name, ', ') WITHIN GROUP (ORDER BY sport_medals DESC) 
        OVER (PARTITION BY country_name) AS all_sports_ranked,
    ROW_NUMBER() OVER (PARTITION BY country_name ORDER BY sport_medals DESC) AS sport_rank
FROM (
    SELECT 
        c.region    AS country_name,
        s.sport_name,
        COUNT(*)    AS sport_medals
    FROM FDBO.PG_RESULTS r
    JOIN OLY_REF.COUNTRIES c ON r.noc = c.noc
    JOIN OLY_REF.EVENTS e    ON r.event_id = e.event_id
    JOIN OLY_REF.SPORTS s    ON e.sport_id = s.sport_id
    WHERE r.medal IS NOT NULL
    GROUP BY c.region, s.sport_name
    HAVING COUNT(*) >= 10
)
        """),
    ]
    
    exec_views(cursor, conn, window_views)
    
    print(f"\n{'='*60}")
    print(f"📋 STEP 5: ORDS REST API (CLOUD_10)")
    print(f"   9 endpoint-uri REST")
    print(f"{'='*60}")
    
    ords_blocks = [
        ("Enable FDBO schema for ORDS", """
BEGIN
    ORDS.ENABLE_SCHEMA(
        p_enabled             => TRUE,
        p_schema              => 'FDBO',
        p_url_mapping_type    => 'BASE_PATH',
        p_url_mapping_pattern => 'fdbo',
        p_auto_rest_auth      => FALSE
    );
    COMMIT;
END;
        """),
        ("Define olympics module", """
BEGIN
    ORDS.DEFINE_MODULE(
        p_module_name    => 'olympics',
        p_base_path      => '/olympics/',
        p_items_per_page => 25,
        p_status         => 'PUBLISHED',
        p_comments       => 'Olympics Data REST API - 4 Data Sources'
    );
    COMMIT;
END;
        """),
        ("Template: /sports", """
BEGIN
    ORDS.DEFINE_TEMPLATE(
        p_module_name    => 'olympics',
        p_pattern        => 'sports',
        p_comments       => 'Lista sporturilor olimpice (DS1-Oracle)'
    );
    ORDS.DEFINE_HANDLER(
        p_module_name    => 'olympics',
        p_pattern        => 'sports',
        p_method         => 'GET',
        p_source_type    => 'json/collection',
        p_source         => 'SELECT sport_id, sport_name, data_source FROM FDBO.FDBO_SPORTS_V ORDER BY sport_name'
    );
    COMMIT;
END;
        """),
        ("Template: /games", """
BEGIN
    ORDS.DEFINE_TEMPLATE(
        p_module_name    => 'olympics',
        p_pattern        => 'games',
        p_comments       => 'Lista editiilor olimpice (DS1-Oracle)'
    );
    ORDS.DEFINE_HANDLER(
        p_module_name    => 'olympics',
        p_pattern        => 'games',
        p_method         => 'GET',
        p_source_type    => 'json/collection',
        p_source         => 'SELECT game_id, games_name, year, season, city, data_source FROM FDBO.FDBO_GAMES_V ORDER BY year'
    );
    COMMIT;
END;
        """),
        ("Template: /countries", """
BEGIN
    ORDS.DEFINE_TEMPLATE(
        p_module_name    => 'olympics',
        p_pattern        => 'countries',
        p_comments       => 'Lista tarilor (DS1-Oracle)'
    );
    ORDS.DEFINE_HANDLER(
        p_module_name    => 'olympics',
        p_pattern        => 'countries',
        p_method         => 'GET',
        p_source_type    => 'json/collection',
        p_source         => 'SELECT noc, region, notes, data_source FROM FDBO.FDBO_COUNTRIES_V ORDER BY region'
    );
    COMMIT;
END;
        """),
        ("Template: /top-medalists", """
BEGIN
    ORDS.DEFINE_TEMPLATE(
        p_module_name    => 'olympics',
        p_pattern        => 'top-medalists',
        p_comments       => 'Top medalisti global (Window Functions)'
    );
    ORDS.DEFINE_HANDLER(
        p_module_name    => 'olympics',
        p_pattern        => 'top-medalists',
        p_method         => 'GET',
        p_source_type    => 'json/collection',
        p_source         => 'SELECT athlete_name, country_name, total_medals, gold, global_dense_rank FROM FDBO.WF_TOP_MEDALISTS_V WHERE global_dense_rank <= 50 ORDER BY global_dense_rank'
    );
    COMMIT;
END;
        """),
        ("Template: /medal-share", """
BEGIN
    ORDS.DEFINE_TEMPLATE(
        p_module_name    => 'olympics',
        p_pattern        => 'medal-share',
        p_comments       => 'Procentul de medalii per tara (Window Functions)'
    );
    ORDS.DEFINE_HANDLER(
        p_module_name    => 'olympics',
        p_pattern        => 'medal-share',
        p_method         => 'GET',
        p_source_type    => 'json/collection',
        p_source         => 'SELECT country_name, total_medals, gold, pct_of_all_medals, pct_of_all_gold, rank_total FROM FDBO.WF_MEDAL_SHARE_V ORDER BY rank_total FETCH FIRST 50 ROWS ONLY'
    );
    COMMIT;
END;
        """),
        ("Template: /olap/country-medals", """
BEGIN
    ORDS.DEFINE_TEMPLATE(
        p_module_name    => 'olympics',
        p_pattern        => 'olap/country-medals',
        p_comments       => 'Medalii per tara cu subtotaluri (ROLLUP)'
    );
    ORDS.DEFINE_HANDLER(
        p_module_name    => 'olympics',
        p_pattern        => 'olap/country-medals',
        p_method         => 'GET',
        p_source_type    => 'json/collection',
        p_source         => 'SELECT country_name, sport_name, total_medals, gold, silver, bronze, is_country_total, is_sport_total FROM FDBO.OLAP_ROLLUP_COUNTRY_SPORT_V WHERE is_sport_total = 1 AND is_country_total = 0 ORDER BY total_medals DESC FETCH FIRST 30 ROWS ONLY'
    );
    COMMIT;
END;
        """),
        ("Template: /data-sources", """
BEGIN
    ORDS.DEFINE_TEMPLATE(
        p_module_name    => 'olympics',
        p_pattern        => 'data-sources',
        p_comments       => 'Statistici per sursa de date (GROUPING SETS)'
    );
    ORDS.DEFINE_HANDLER(
        p_module_name    => 'olympics',
        p_pattern        => 'data-sources',
        p_method         => 'GET',
        p_source_type    => 'json/collection',
        p_source         => 'SELECT source_name, category, record_count, grouping_id FROM FDBO.OLAP_GS_DATA_SOURCES_V ORDER BY grouping_id, source_name'
    );
    COMMIT;
END;
        """),
        ("Template: /json/athlete-medals", """
BEGIN
    ORDS.DEFINE_TEMPLATE(
        p_module_name    => 'olympics',
        p_pattern        => 'json/athlete-medals',
        p_comments       => 'Documente JSON medalii atleti (DS3-MongoDB)'
    );
    ORDS.DEFINE_HANDLER(
        p_module_name    => 'olympics',
        p_pattern        => 'json/athlete-medals',
        p_method         => 'GET',
        p_source_type    => 'json/collection',
        p_source         => 'SELECT athlete_name, noc, total_medals, gold_medals, silver_medals, bronze_medals, sports_list, data_source FROM FDBO.FDBO_JSON_ATHLETE_MEDALS_V ORDER BY total_medals DESC FETCH FIRST 50 ROWS ONLY'
    );
    COMMIT;
END;
        """),
        ("Template: /growth", """
BEGIN
    ORDS.DEFINE_TEMPLATE(
        p_module_name    => 'olympics',
        p_pattern        => 'growth',
        p_comments       => 'Cresterea Jocurilor Olimpice (LAG/LEAD Window Functions)'
    );
    ORDS.DEFINE_HANDLER(
        p_module_name    => 'olympics',
        p_pattern        => 'growth',
        p_method         => 'GET',
        p_source_type    => 'json/collection',
        p_source         => 'SELECT year, season, city, total_athletes, prev_edition_athletes, growth, growth_pct FROM FDBO.WF_LAG_LEAD_EDITIONS_V ORDER BY year'
    );
    COMMIT;
END;
        """),
    ]
    
    exec_plsql(cursor, conn, ords_blocks)
    
    cursor.close()

def exec_views(cursor, conn, views_list):
    """Execută o listă de CREATE VIEW statements."""
    success = 0
    errors = 0
    for name, sql in views_list:
        try:
            cursor.execute(sql.strip())
            conn.commit()
            print(f"   ✅ {name}")
            success += 1
        except oracledb.DatabaseError as e:
            err = str(e).split('\n')[0][:120]
            print(f"   ❌ {name}")
            print(f"      {err}")
            errors += 1
    
    print(f"\n   📊 Views: {success} OK, {errors} erori")
    return success, errors

def exec_plsql(cursor, conn, blocks_list):
    """Execută o listă de blocuri PL/SQL."""
    success = 0
    errors = 0
    for name, sql in blocks_list:
        try:
            cursor.execute(sql.strip())
            conn.commit()
            print(f"   ✅ {name}")
            success += 1
        except oracledb.DatabaseError as e:
            err = str(e).split('\n')[0][:120]
            print(f"   ❌ {name}")
            print(f"      {err}")
            errors += 1
    
    print(f"\n   📊 ORDS: {success} OK, {errors} erori")
    return success, errors

def verify_views(conn):
    """Verificare finală - listează toate view-urile și testează câteva."""
    cursor = conn.cursor()
    
    print(f"\n{'='*60}")
    print(f"📊 VERIFICARE FINALĂ")
    print(f"{'='*60}")
    
    cursor.execute("SELECT view_name FROM all_views WHERE owner = 'FDBO' ORDER BY view_name")
    views = [r[0] for r in cursor.fetchall()]
    print(f"\n   📋 View-uri FDBO ({len(views)} total):")
    for v in views:
        print(f"      • {v}")
    
    print(f"\n   🔍 Test INT_RESULTS_FULL_V (VIEW URIAȘ):")
    try:
        cursor.execute("SELECT COUNT(*) FROM FDBO.INT_RESULTS_FULL_V")
        count = cursor.fetchone()[0]
        print(f"      Total rânduri: {count:,}")
        
        cursor.execute("SELECT * FROM FDBO.INT_RESULTS_FULL_V WHERE ROWNUM <= 3")
        cols = [d[0] for d in cursor.description]
        print(f"      Coloane ({len(cols)}): {', '.join(cols)}")
    except Exception as e:
        print(f"      ❌ Eroare: {e}")
    
    print(f"\n   🔍 Test OLAP - Top 5 țări medaliate (ROLLUP):")
    try:
        cursor.execute("""
            SELECT country_name, total_medals, gold, silver, bronze 
            FROM FDBO.OLAP_ROLLUP_COUNTRY_SPORT_V 
            WHERE is_sport_total = 1 AND is_country_total = 0
            ORDER BY total_medals DESC
            FETCH FIRST 5 ROWS ONLY
        """)
        for row in cursor.fetchall():
            print(f"      🏅 {row[0]}: {row[1]} medalii (🥇{row[2]} 🥈{row[3]} 🥉{row[4]})")
    except Exception as e:
        print(f"      ❌ Eroare: {e}")
    
    print(f"\n   🔍 Test Window Functions - Top 5 medaliști global:")
    try:
        cursor.execute("""
            SELECT athlete_name, country_name, total_medals, gold, global_dense_rank
            FROM FDBO.WF_TOP_MEDALISTS_V 
            WHERE global_dense_rank <= 5
            ORDER BY global_dense_rank
        """)
        for row in cursor.fetchall():
            print(f"      #{row[4]} {row[0]} ({row[1]}): {row[2]} medalii ({row[3]} gold)")
    except Exception as e:
        print(f"      ❌ Eroare: {e}")
    
    print(f"\n   🔍 Test JSON_TABLE - Top 3 medaliști din MongoDB/JSON:")
    try:
        cursor.execute("""
            SELECT athlete_name, noc, total_medals, gold_medals, sports_list
            FROM FDBO.FDBO_JSON_ATHLETE_MEDALS_V 
            ORDER BY total_medals DESC
            FETCH FIRST 3 ROWS ONLY
        """)
        for row in cursor.fetchall():
            print(f"      🏅 {row[0]} ({row[1]}): {row[2]} medalii, {row[3]} gold - Sports: {row[4][:60]}...")
    except Exception as e:
        print(f"      ❌ Eroare: {e}")
    
    print(f"\n   🔍 Test GROUPING SETS - Data Sources Overview:")
    try:
        cursor.execute("""
            SELECT source_name, category, record_count, grouping_id
            FROM FDBO.OLAP_GS_DATA_SOURCES_V 
            ORDER BY grouping_id, source_name
        """)
        for row in cursor.fetchall():
            src = row[0] if row[0] else 'GRAND TOTAL'
            cat = row[1] if row[1] else '(subtotal)'
            print(f"      📊 [{row[3]}] {src} / {cat}: {row[2]:,}")
    except Exception as e:
        print(f"      ❌ Eroare: {e}")
    
    print(f"\n   🇷🇴 România:")
    try:
        cursor.execute("""
            SELECT country_name, total_medals, gold, pct_of_all_medals, rank_total
            FROM FDBO.WF_MEDAL_SHARE_V 
            WHERE country_name = 'Romania'
        """)
        row = cursor.fetchone()
        if row:
            print(f"      {row[0]}: {row[1]} medalii, {row[2]} gold, {row[3]}% din total, Rank #{row[4]}")
        else:
            print(f"      Nu s-a găsit")
    except Exception as e:
        print(f"      ❌ Eroare: {e}")
    
    print(f"\n   🌐 ORDS Modules:")
    try:
        cursor.execute("SELECT id, name, uri_prefix, status FROM user_ords_modules")
        for row in cursor.fetchall():
            print(f"      🔗 {row[1]} - prefix: {row[2]} - status: {row[3]}")
    except Exception as e:
        print(f"      ⚠️ ORDS: {e}")
    
    cursor.close()

def main():
    print("=" * 60)
    print("🏟️  OLYMPICS DB - Creare Views + ORDS REST API")
    print("=" * 60)
    
    start = time.time()
    conn = get_connection()
    
    cursor = conn.cursor()
    print("\n📋 Verificare grant-uri...")
    grants = [
        "GRANT SELECT ON OLY_REF.SPORTS TO FDBO",
        "GRANT SELECT ON OLY_REF.EVENTS TO FDBO",
        "GRANT SELECT ON OLY_REF.GAMES TO FDBO",
        "GRANT SELECT ON OLY_REF.COUNTRIES TO FDBO",
        "GRANT CREATE VIEW TO FDBO",
    ]
    for g in grants:
        try:
            cursor.execute(g)
            print(f"   ✅ {g}")
        except oracledb.DatabaseError as e:
            if 'ORA-01927' in str(e):  # grant already exists
                print(f"   ⚠️ {g} (deja acordat)")
            else:
                print(f"   ❌ {g}: {e}")
    conn.commit()
    cursor.close()
    
    execute_views_directly(conn)
    
    verify_views(conn)
    
    elapsed = time.time() - start
    print(f"\n⏱️  Timp total: {elapsed:.1f} secunde")
    print("🏁 GATA!")
    
    conn.close()

if __name__ == "__main__":
    main()
