"""
=============================================================
CLOUD SETUP: Creează useri și tabele în Oracle Cloud
Rulează ÎNAINTE de load_to_oracle.py
=============================================================
"""
import oracledb
import sys

WALLET_DIR = r"C:\Users\Razvan.PLETOSU\Downloads\Wallet_OlympicsDB"

def get_conn():
    return oracledb.connect(
        user='ADMIN',
        password='Oracle_1234#',
        dsn='olympicsdb_low',
        config_dir=WALLET_DIR,
        wallet_location=WALLET_DIR,
        wallet_password='Oracle_1234#'
    )

def execute_sql(conn, sql, desc=""):
    """Execută un statement SQL, ignoră erori de 'already exists'"""
    cur = conn.cursor()
    try:
        cur.execute(sql)
        print(f"  ✅ {desc}")
    except oracledb.DatabaseError as e:
        err = str(e)
        if any(x in err for x in ['ORA-01920', 'ORA-00955', 'ORA-01430', 'ORA-01921', 'ORA-02261', 'already exists']):
            print(f"  ⏭️  {desc} (deja există)")
        else:
            print(f"  ❌ {desc}: {e}")
            raise

def step1_create_users(conn):
    print("\n" + "="*60)
    print("STEP 1: Creare useri OLY_REF și FDBO")
    print("="*60)
    
    execute_sql(conn, """
        CREATE USER OLY_REF IDENTIFIED BY "Oracle_1234#"
        DEFAULT TABLESPACE DATA
        QUOTA UNLIMITED ON DATA
    """, "CREATE USER OLY_REF")
    
    execute_sql(conn, "GRANT CONNECT, RESOURCE TO OLY_REF", "GRANT CONNECT, RESOURCE TO OLY_REF")
    execute_sql(conn, "GRANT CREATE VIEW TO OLY_REF", "GRANT CREATE VIEW TO OLY_REF")
    execute_sql(conn, "GRANT CREATE TABLE TO OLY_REF", "GRANT CREATE TABLE TO OLY_REF")
    execute_sql(conn, "GRANT CREATE SESSION TO OLY_REF", "GRANT CREATE SESSION TO OLY_REF")
    
    execute_sql(conn, """
        CREATE USER FDBO IDENTIFIED BY "Oracle_1234#"
        DEFAULT TABLESPACE DATA
        QUOTA UNLIMITED ON DATA
    """, "CREATE USER FDBO")
    
    execute_sql(conn, "GRANT CONNECT, RESOURCE TO FDBO", "GRANT CONNECT, RESOURCE TO FDBO")
    execute_sql(conn, "GRANT CREATE VIEW TO FDBO", "GRANT CREATE VIEW TO FDBO")
    execute_sql(conn, "GRANT CREATE TABLE TO FDBO", "GRANT CREATE TABLE TO FDBO")
    execute_sql(conn, "GRANT CREATE SESSION TO FDBO", "GRANT CREATE SESSION TO FDBO")
    
    conn.commit()

def step2_create_oly_ref_tables(conn):
    print("\n" + "="*60)
    print("STEP 2: Creare tabele OLY_REF")
    print("="*60)
    
    execute_sql(conn, """
        CREATE TABLE OLY_REF.SPORTS (
            sport_id   NUMBER(10)    PRIMARY KEY,
            sport_name VARCHAR2(100) NOT NULL
        )
    """, "CREATE TABLE OLY_REF.SPORTS")
    
    execute_sql(conn, """
        CREATE TABLE OLY_REF.EVENTS (
            event_id   NUMBER(10)    PRIMARY KEY,
            sport_id   NUMBER(10)    NOT NULL,
            event_name VARCHAR2(200) NOT NULL,
            CONSTRAINT FK_EVENTS_SPORT FOREIGN KEY (sport_id) REFERENCES OLY_REF.SPORTS(sport_id)
        )
    """, "CREATE TABLE OLY_REF.EVENTS")
    
    execute_sql(conn, """
        CREATE TABLE OLY_REF.GAMES (
            game_id    NUMBER(10)    PRIMARY KEY,
            games_name VARCHAR2(50)  NOT NULL,
            year       NUMBER(4)     NOT NULL,
            season     VARCHAR2(10)  NOT NULL,
            city       VARCHAR2(100) NOT NULL
        )
    """, "CREATE TABLE OLY_REF.GAMES")
    
    execute_sql(conn, """
        CREATE TABLE OLY_REF.COUNTRIES (
            noc    VARCHAR2(10)  PRIMARY KEY,
            region VARCHAR2(200) NOT NULL,
            notes  VARCHAR2(500)
        )
    """, "CREATE TABLE OLY_REF.COUNTRIES")
    
    execute_sql(conn, "GRANT SELECT ON OLY_REF.SPORTS TO FDBO", "GRANT SELECT SPORTS -> FDBO")
    execute_sql(conn, "GRANT SELECT ON OLY_REF.EVENTS TO FDBO", "GRANT SELECT EVENTS -> FDBO")
    execute_sql(conn, "GRANT SELECT ON OLY_REF.GAMES TO FDBO", "GRANT SELECT GAMES -> FDBO")
    execute_sql(conn, "GRANT SELECT ON OLY_REF.COUNTRIES TO FDBO", "GRANT SELECT COUNTRIES -> FDBO")
    
    conn.commit()

def step3_create_fdbo_tables(conn):
    print("\n" + "="*60)
    print("STEP 3: Creare tabele FDBO")
    print("="*60)
    
    execute_sql(conn, """
        CREATE TABLE FDBO.PG_ATHLETES (
            athlete_id   NUMBER(10)    PRIMARY KEY,
            full_name    VARCHAR2(300) NOT NULL,
            sex          VARCHAR2(1),
            birth_year   NUMBER(4),
            birth_place  VARCHAR2(200),
            height_cm    NUMBER(5,1),
            weight_kg    NUMBER(5,1),
            noc          VARCHAR2(10),
            data_source  VARCHAR2(20) DEFAULT 'PostgreSQL'
        )
    """, "CREATE TABLE FDBO.PG_ATHLETES")
    
    execute_sql(conn, """
        CREATE TABLE FDBO.PG_RESULTS (
            result_id    NUMBER(10)    PRIMARY KEY,
            athlete_id   NUMBER(10),
            game_id      NUMBER(10),
            event_id     NUMBER(10),
            medal        VARCHAR2(10),
            age          NUMBER(3),
            team         VARCHAR2(200),
            noc          VARCHAR2(10),
            data_source  VARCHAR2(20) DEFAULT 'PostgreSQL',
            CONSTRAINT FK_PG_RES_ATHLETE FOREIGN KEY (athlete_id) REFERENCES FDBO.PG_ATHLETES(athlete_id)
        )
    """, "CREATE TABLE FDBO.PG_RESULTS")
    
    execute_sql(conn, "CREATE INDEX FDBO.IDX_PG_RES_ATHLETE ON FDBO.PG_RESULTS(athlete_id)", "INDEX PG_RESULTS(athlete_id)")
    execute_sql(conn, "CREATE INDEX FDBO.IDX_PG_RES_GAME ON FDBO.PG_RESULTS(game_id)", "INDEX PG_RESULTS(game_id)")
    execute_sql(conn, "CREATE INDEX FDBO.IDX_PG_RES_EVENT ON FDBO.PG_RESULTS(event_id)", "INDEX PG_RESULTS(event_id)")
    execute_sql(conn, "CREATE INDEX FDBO.IDX_PG_RES_MEDAL ON FDBO.PG_RESULTS(medal)", "INDEX PG_RESULTS(medal)")
    execute_sql(conn, "CREATE INDEX FDBO.IDX_PG_ATH_NOC ON FDBO.PG_ATHLETES(noc)", "INDEX PG_ATHLETES(noc)")
    
    execute_sql(conn, """
        CREATE TABLE FDBO.JSON_MEDAL_DOCS (
            doc_id       NUMBER(10) GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            doc_type     VARCHAR2(30)  NOT NULL,
            json_data    CLOB CHECK (json_data IS JSON),
            created_at   TIMESTAMP DEFAULT SYSTIMESTAMP,
            data_source  VARCHAR2(20) DEFAULT 'MongoDB/JSON'
        )
    """, "CREATE TABLE FDBO.JSON_MEDAL_DOCS")
    
    execute_sql(conn, "CREATE INDEX FDBO.IDX_JSON_DOC_TYPE ON FDBO.JSON_MEDAL_DOCS(doc_type)", "INDEX JSON_MEDAL_DOCS(doc_type)")
    
    execute_sql(conn, """
        CREATE TABLE FDBO.CSV_ATHLETE_EVENTS (
            row_id         NUMBER(10) GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            athlete_name   VARCHAR2(300),
            sex            VARCHAR2(1),
            age            NUMBER(3),
            height         NUMBER(5,1),
            weight         NUMBER(5,1),
            team           VARCHAR2(200),
            noc            VARCHAR2(10),
            games          VARCHAR2(50),
            year           NUMBER(4),
            season         VARCHAR2(10),
            city           VARCHAR2(100),
            sport          VARCHAR2(100),
            event          VARCHAR2(200),
            medal          VARCHAR2(10),
            region         VARCHAR2(200),
            data_source    VARCHAR2(20) DEFAULT 'CSV'
        )
    """, "CREATE TABLE FDBO.CSV_ATHLETE_EVENTS")
    
    execute_sql(conn, "CREATE INDEX FDBO.IDX_CSV_NOC ON FDBO.CSV_ATHLETE_EVENTS(noc)", "INDEX CSV_ATHLETE_EVENTS(noc)")
    execute_sql(conn, "CREATE INDEX FDBO.IDX_CSV_YEAR ON FDBO.CSV_ATHLETE_EVENTS(year)", "INDEX CSV_ATHLETE_EVENTS(year)")
    execute_sql(conn, "CREATE INDEX FDBO.IDX_CSV_MEDAL ON FDBO.CSV_ATHLETE_EVENTS(medal)", "INDEX CSV_ATHLETE_EVENTS(medal)")
    execute_sql(conn, "CREATE INDEX FDBO.IDX_CSV_SPORT ON FDBO.CSV_ATHLETE_EVENTS(sport)", "INDEX CSV_ATHLETE_EVENTS(sport)")
    
    conn.commit()

def verify(conn):
    print("\n" + "="*60)
    print("VERIFICARE")
    print("="*60)
    cur = conn.cursor()
    
    cur.execute("SELECT username FROM all_users WHERE username IN ('OLY_REF','FDBO') ORDER BY username")
    users = [r[0] for r in cur]
    print(f"  Useri: {users}")
    
    cur.execute("SELECT owner, table_name FROM all_tables WHERE owner IN ('OLY_REF','FDBO') ORDER BY owner, table_name")
    for r in cur:
        print(f"  {r[0]}.{r[1]}")

def main():
    print("="*60)
    print("🏟️  OLYMPICS DB - Setup Oracle Cloud")
    print("="*60)
    
    conn = get_conn()
    print(f"Conectat: {conn.version}")
    
    try:
        step1_create_users(conn)
        step2_create_oly_ref_tables(conn)
        step3_create_fdbo_tables(conn)
        verify(conn)
    finally:
        conn.close()
    
    print("\n✅ Setup complet! Acum rulează: python load_to_oracle.py")

if __name__ == '__main__':
    main()
