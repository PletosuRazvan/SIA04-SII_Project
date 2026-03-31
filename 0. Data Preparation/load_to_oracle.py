"""
=============================================================
CLOUD STEP 5: Python Script - Încarcă date în Oracle Cloud
Folosește python-oracledb (thin mode) pentru Oracle Autonomous DB
=============================================================
Rulează: python load_to_oracle.py

Prerequisite:
  pip install oracledb pandas

Acest script încarcă:
  1. PG_ATHLETES din pg_data/athletes.csv
  2. PG_RESULTS din pg_data/results.csv
  3. JSON_MEDAL_DOCS din mongo_data/*.jsonl
  4. CSV_ATHLETE_EVENTS din csv_data/athlete_events_enriched.csv
  5. EVENTS rămase din oracle_data/insert_events.sql
"""

import oracledb
import pandas as pd
import json
import os
import time

WALLET_DIR = r"C:\Users\Razvan.PLETOSU\Downloads\Wallet_OlympicsDB"
WALLET_PASSWORD = "Oracle_1234#"

DSN = "olympicsdb_low"  # Numele din tnsnames.ora din wallet
USER = "ADMIN"
PASSWORD = "Oracle_1234#"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PG_DATA_DIR = os.path.join(BASE_DIR, "pg_data")
MONGO_DATA_DIR = os.path.join(BASE_DIR, "mongo_data")
CSV_DATA_DIR = os.path.join(BASE_DIR, "csv_data")
ORACLE_DATA_DIR = os.path.join(BASE_DIR, "oracle_data")

BATCH_SIZE = 1000

def get_connection():
    """Conectare la Oracle Cloud Autonomous Database"""
    print(f"Conectare la Oracle Cloud ca {USER}...")
    print(f"Wallet: {WALLET_DIR}")
    
    conn = oracledb.connect(
        user=USER,
        password=PASSWORD,
        dsn=DSN,
        config_dir=WALLET_DIR,
        wallet_location=WALLET_DIR,
        wallet_password=WALLET_PASSWORD
    )
    print(f"✅ Conectat: {conn.version}")
    return conn

def load_ref_data(conn):
    """Încarcă SPORTS, GAMES, COUNTRIES din fișierele SQL"""
    print("\n" + "="*60)
    print("📋 Încărcare date referință (SPORTS, GAMES, COUNTRIES)...")
    
    cursor = conn.cursor()
    
    for table, filename in [
        ('SPORTS', 'insert_sports.sql'),
        ('GAMES', 'insert_games.sql'),
        ('COUNTRIES', 'insert_countries.sql'),
    ]:
        cursor.execute(f"SELECT COUNT(*) FROM OLY_REF.{table}")
        existing = cursor.fetchone()[0]
        if existing > 0:
            print(f"   ⏭️  OLY_REF.{table} are deja {existing} rânduri")
            continue
        
        sql_file = os.path.join(ORACLE_DATA_DIR, filename)
        if not os.path.exists(sql_file):
            print(f"   ⚠️ {sql_file} nu există")
            continue
        
        with open(sql_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        count = 0
        for line in content.split('\n'):
            line = line.strip()
            if line.startswith('INSERT INTO OLY_REF.'):
                try:
                    cursor.execute(line.rstrip(';'))
                    count += 1
                except Exception as e:
                    if 'ORA-00001' in str(e):
                        pass  # duplicate
                    else:
                        print(f"   ⚠️ {e}")
        conn.commit()
        print(f"   ✅ OLY_REF.{table}: {count} rânduri inserate")

def load_events(conn):
    """Încarcă EVENTS (765 rows) - DUPĂ SPORTS"""
    print("\n" + "="*60)
    print("📋 Încărcare EVENTS (oracle_data/insert_events.sql)...")
    
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM OLY_REF.EVENTS")
    existing = cursor.fetchone()[0]
    
    if existing >= 765:
        print(f"   ✅ Toate events-urile sunt deja încărcate ({existing})")
        return
    
    sql_file = os.path.join(ORACLE_DATA_DIR, "insert_events.sql")
    if not os.path.exists(sql_file):
        print(f"   ⚠️ Fișier nu există: {sql_file}")
        return
    
    with open(sql_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    count = 0
    for line in content.split('\n'):
        line = line.strip()
        if line.startswith('INSERT INTO OLY_REF.EVENTS'):
            try:
                cursor.execute(line.rstrip(';'))
                count += 1
                if count % 100 == 0:
                    conn.commit()
                    print(f"   ... {count} events inserate")
            except Exception as e:
                if 'ORA-00001' in str(e):
                    pass  # duplicate
                else:
                    print(f"   ⚠️ Eroare la event: {e}")
    
    conn.commit()
    cursor.execute("SELECT COUNT(*) FROM OLY_REF.EVENTS")
    total = cursor.fetchone()[0]
    print(f"   ✅ Total events: {total}")

def load_pg_athletes(conn):
    """Încarcă athletes din CSV în FDBO.PG_ATHLETES"""
    print("\n" + "="*60)
    print("🏃 Încărcare PG_ATHLETES din pg_data/athletes.csv...")
    
    csv_path = os.path.join(PG_DATA_DIR, "athletes.csv")
    if not os.path.exists(csv_path):
        print(f"   ⚠️ Fișier nu există: {csv_path}")
        return
    
    df = pd.read_csv(csv_path)
    print(f"   Citite {len(df)} rânduri din CSV")
    
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM FDBO.PG_ATHLETES")
    existing = cursor.fetchone()[0]
    if existing > 0:
        print(f"   ⚠️ Tabelul are deja {existing} rânduri. Skip.")
        return
    
    sql = """INSERT INTO FDBO.PG_ATHLETES 
             (athlete_id, full_name, sex, height_cm, weight_kg)
             VALUES (:1, :2, :3, :4, :5)"""
    
    rows = []
    count = 0
    for _, r in df.iterrows():
        rows.append((
            int(r['athlete_id']),
            str(r['name'])[:300] if pd.notna(r['name']) else 'Unknown',
            str(r['sex'])[:1] if pd.notna(r.get('sex')) else None,
            float(r['height']) if pd.notna(r.get('height')) else None,
            float(r['weight']) if pd.notna(r.get('weight')) else None,
        ))
        
        if len(rows) >= BATCH_SIZE:
            cursor.executemany(sql, rows)
            conn.commit()
            count += len(rows)
            print(f"   ... {count} inserate")
            rows = []
    
    if rows:
        cursor.executemany(sql, rows)
        conn.commit()
        count += len(rows)
    
    cursor.execute("SELECT COUNT(*) FROM FDBO.PG_ATHLETES")
    total = cursor.fetchone()[0]
    print(f"   ✅ Total PG_ATHLETES: {total}")

def load_pg_results(conn):
    """Încarcă results din CSV în FDBO.PG_RESULTS"""
    print("\n" + "="*60)
    print("🏅 Încărcare PG_RESULTS din pg_data/results.csv...")
    
    csv_path = os.path.join(PG_DATA_DIR, "results.csv")
    if not os.path.exists(csv_path):
        print(f"   ⚠️ Fișier nu există: {csv_path}")
        return
    
    df = pd.read_csv(csv_path)
    print(f"   Citite {len(df)} rânduri din CSV")
    
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM FDBO.PG_RESULTS")
    existing = cursor.fetchone()[0]
    if existing > 0:
        print(f"   ⚠️ Tabelul are deja {existing} rânduri. Skip.")
        return
    
    sql = """INSERT INTO FDBO.PG_RESULTS 
             (result_id, athlete_id, game_id, event_id, medal, age, team, noc)
             VALUES (:1, :2, :3, :4, :5, :6, :7, :8)"""
    
    rows = []
    count = 0
    for _, r in df.iterrows():
        medal_val = str(r.get('medal', '')) if pd.notna(r.get('medal')) else None
        if medal_val in ('nan', 'None', 'NA', ''):
            medal_val = None
        
        age_val = None
        if pd.notna(r.get('age')):
            try:
                age_val = int(round(float(r['age'])))
            except (ValueError, TypeError):
                pass
        
        rows.append((
            int(r['result_id']),
            int(r['athlete_id']),
            int(r['game_id']) if pd.notna(r.get('game_id')) else None,
            int(r['event_id']) if pd.notna(r.get('event_id')) else None,
            medal_val[:10] if medal_val else None,
            age_val,
            str(r['team'])[:200] if pd.notna(r.get('team')) else None,
            str(r['noc'])[:10] if pd.notna(r.get('noc')) else None
        ))
        
        if len(rows) >= BATCH_SIZE:
            cursor.executemany(sql, rows)
            conn.commit()
            count += len(rows)
            print(f"   ... {count} inserate")
            rows = []
    
    if rows:
        cursor.executemany(sql, rows)
        conn.commit()
        count += len(rows)
    
    cursor.execute("SELECT COUNT(*) FROM FDBO.PG_RESULTS")
    total = cursor.fetchone()[0]
    print(f"   ✅ Total PG_RESULTS: {total}")

def load_json_docs(conn):
    """Încarcă documente JSON în FDBO.JSON_MEDAL_DOCS"""
    print("\n" + "="*60)
    print("📄 Încărcare JSON_MEDAL_DOCS din mongo_data/*.jsonl...")
    
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM FDBO.JSON_MEDAL_DOCS")
    existing = cursor.fetchone()[0]
    if existing > 0:
        print(f"   ⚠️ Tabelul are deja {existing} documente. Skip.")
        return
    
    sql = """INSERT INTO FDBO.JSON_MEDAL_DOCS (doc_type, json_data) 
             VALUES (:1, :2)"""
    
    total = 0
    
    path1 = os.path.join(MONGO_DATA_DIR, "athlete_medals_docs.jsonl")
    if os.path.exists(path1):
        rows = []
        with open(path1, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(('athlete_medal', line))
                    if len(rows) >= BATCH_SIZE:
                        cursor.executemany(sql, rows)
                        conn.commit()
                        total += len(rows)
                        print(f"   ... {total} docs athlete_medal inserate")
                        rows = []
        if rows:
            cursor.executemany(sql, rows)
            conn.commit()
            total += len(rows)
        print(f"   ✅ athlete_medal docs: {total}")
    
    path2 = os.path.join(MONGO_DATA_DIR, "game_summary_docs.jsonl")
    if os.path.exists(path2):
        count2 = 0
        rows = []
        with open(path2, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(('game_summary', line))
        if rows:
            cursor.executemany(sql, rows)
            conn.commit()
            count2 = len(rows)
            total += count2
        print(f"   ✅ game_summary docs: {count2}")
    
    cursor.execute("SELECT COUNT(*) FROM FDBO.JSON_MEDAL_DOCS")
    final = cursor.fetchone()[0]
    print(f"   ✅ Total JSON_MEDAL_DOCS: {final}")

def load_csv_enriched(conn):
    """Încarcă CSV enriched în FDBO.CSV_ATHLETE_EVENTS"""
    print("\n" + "="*60)
    print("📊 Încărcare CSV_ATHLETE_EVENTS din csv_data/athlete_events_enriched.csv...")
    
    csv_path = os.path.join(CSV_DATA_DIR, "athlete_events_enriched.csv")
    if not os.path.exists(csv_path):
        print(f"   ⚠️ Fișier nu există: {csv_path}")
        return
    
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM FDBO.CSV_ATHLETE_EVENTS")
    existing = cursor.fetchone()[0]
    if existing > 0:
        print(f"   ⚠️ Tabelul are deja {existing} rânduri. Skip.")
        return
    
    MAX_CSV_ROWS = 50000
    df = pd.read_csv(csv_path, nrows=MAX_CSV_ROWS)
    print(f"   Citite {len(df)} rânduri din CSV (max {MAX_CSV_ROWS} pentru demo)")
    
    sql = """INSERT INTO FDBO.CSV_ATHLETE_EVENTS 
             (athlete_name, sex, age, height, weight, team, noc, 
              games, year, season, city, sport, event, medal, region)
             VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15)"""
    
    rows = []
    count = 0
    for _, r in df.iterrows():
        def safe_str(val, maxlen=200):
            if pd.isna(val) or str(val) == 'nan':
                return None
            return str(val)[:maxlen]
        
        def safe_int(val):
            if pd.isna(val) or str(val) == 'nan':
                return None
            return int(float(val))
        
        def safe_float(val):
            if pd.isna(val) or str(val) == 'nan':
                return None
            return float(val)
        
        medal_val = safe_str(r.get('medal', None), 10)
        if medal_val in ('NA', 'nan', 'None', ''):
            medal_val = None
        
        rows.append((
            safe_str(r.get('name', r.get('athlete_name', '')), 300),
            safe_str(r.get('sex'), 1),
            safe_int(r.get('age')),
            safe_float(r.get('height')),
            safe_float(r.get('weight')),
            safe_str(r.get('team'), 200),
            safe_str(r.get('noc'), 10),
            safe_str(r.get('games'), 50),
            safe_int(r.get('year')),
            safe_str(r.get('season'), 10),
            safe_str(r.get('city'), 100),
            safe_str(r.get('sport'), 100),
            safe_str(r.get('event'), 200),
            medal_val,
            safe_str(r.get('region'), 200)
        ))
        
        if len(rows) >= BATCH_SIZE:
            cursor.executemany(sql, rows)
            conn.commit()
            count += len(rows)
            print(f"   ... {count} inserate")
            rows = []
    
    if rows:
        cursor.executemany(sql, rows)
        conn.commit()
        count += len(rows)
    
    cursor.execute("SELECT COUNT(*) FROM FDBO.CSV_ATHLETE_EVENTS")
    total = cursor.fetchone()[0]
    print(f"   ✅ Total CSV_ATHLETE_EVENTS: {total}")

def verify_all(conn):
    """Verifică toate tabelele"""
    print("\n" + "="*60)
    print("📊 VERIFICARE FINALĂ")
    print("="*60)
    
    cursor = conn.cursor()
    tables = [
        ('OLY_REF', 'SPORTS'),
        ('OLY_REF', 'GAMES'),
        ('OLY_REF', 'EVENTS'),
        ('OLY_REF', 'COUNTRIES'),
        ('FDBO', 'PG_ATHLETES'),
        ('FDBO', 'PG_RESULTS'),
        ('FDBO', 'JSON_MEDAL_DOCS'),
        ('FDBO', 'CSV_ATHLETE_EVENTS'),
    ]
    
    for owner, table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {owner}.{table}")
            count = cursor.fetchone()[0]
            status = "✅" if count > 0 else "❌"
            print(f"   {status} {owner}.{table}: {count:,} rânduri")
        except Exception as e:
            print(f"   ❌ {owner}.{table}: EROARE - {e}")
    
    print("\n   📌 Surse de date:")
    print("   DS1 (Oracle)     : OLY_REF.SPORTS, GAMES, EVENTS, COUNTRIES")
    print("   DS2 (PostgreSQL) : FDBO.PG_ATHLETES, PG_RESULTS")
    print("   DS3 (MongoDB/JSON): FDBO.JSON_MEDAL_DOCS")
    print("   DS4 (CSV)        : FDBO.CSV_ATHLETE_EVENTS")

def main():
    start = time.time()
    print("="*60)
    print("🏟️  OLYMPICS DB - Încărcare date în Oracle Cloud")
    print("="*60)
    
    conn = get_connection()
    
    try:
        load_ref_data(conn)
        load_events(conn)
        load_pg_athletes(conn)
        load_pg_results(conn)
        load_json_docs(conn)
        load_csv_enriched(conn)
        verify_all(conn)
    finally:
        conn.close()
    
    elapsed = time.time() - start
    print(f"\n⏱️ Timp total: {elapsed:.1f} secunde")
    print("🏁 GATA!")

if __name__ == '__main__':
    main()
