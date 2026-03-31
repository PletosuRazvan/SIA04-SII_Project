# 🏟️ Olympics Integrated Data System (SII Project)

## Proiect Sisteme Informaționale Integrate - Baze de Date Olimpice

### 📋 Descriere

Sistem integrat de date care combină **4 surse de date distincte** într-un singur **VIEW URIAȘ** (271,116 rânduri, 22 coloane) folosind Oracle Cloud Autonomous Database 26ai.

**Tema**: 120 Years of Olympic History (1896-2016) — date despre atleți, rezultate, medalii, sporturi și ediții olimpice.

**Dataset**: [120 Years of Olympic History](https://www.kaggle.com/datasets/heesoo37/120-years-of-olympic-history-athletes-and-results) - Kaggle

Descarcă cele 2 fișiere CSV:
- `athlete_events.csv` (~271.000 rânduri × 15 coloane)
- `noc_regions.csv` (230 rânduri × 3 coloane)

### 🏗️ Arhitectură

```
┌─────────────────────────────────────────────────────────┐
│              Oracle Cloud Autonomous DB 26ai             │
│                    (OlympicsDB)                          │
│                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │  DS1: Oracle  │  │DS2: PostgreSQL│  │DS3: MongoDB  │   │
│  │  (OLY_REF)   │  │   (FDBO.PG_*) │  │(FDBO.JSON_*) │   │
│  │              │  │              │  │              │   │
│  │ • SPORTS  66 │  │• PG_ATHLETES │  │• JSON_MEDAL  │   │
│  │ • EVENTS 765 │  │  135,571     │  │  _DOCS       │   │
│  │ • GAMES   52 │  │• PG_RESULTS  │  │  28,302      │   │
│  │ • COUNTRIES  │  │  271,116     │  │              │   │
│  │   230       │  │              │  │              │   │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘   │
│         │                 │                 │            │
│  ┌──────┴─────────────────┴─────────────────┴───────┐   │
│  │           FDBO Access Views (JSON_TABLE)          │   │
│  └──────────────────────┬────────────────────────────┘   │
│                         │                                │
│  ┌──────────────────────┴────────────────────────────┐   │
│  │      INT_RESULTS_FULL_V (VIEW URIAȘ - 271K)      │   │
│  │   + DIM_ATHLETE_V + DIM_GAME_V + DIM_EVENT_V     │   │
│  │   + DIM_COUNTRY_V + FACT_RESULTS_V                │   │
│  └──────────────────────┬────────────────────────────┘   │
│                         │                                │
│  ┌───────────┐ ┌────────┴──────┐ ┌────────────────────┐ │
│  │ 3 ROLLUP  │ │ 3 CUBE       │ │ 3 GROUPING SETS    │ │
│  │ Views     │ │ Views        │ │ Views              │ │
│  └───────────┘ └───────────────┘ └────────────────────┘ │
│                                                          │
│  ┌──────────────────────────────────────────────────┐   │
│  │  8 Window Function Views (ROW_NUMBER, RANK,       │   │
│  │  DENSE_RANK, LAG, LEAD, NTILE, RATIO_TO_REPORT, │   │
│  │  FIRST_VALUE, LAST_VALUE, LISTAGG, SUM, AVG)     │   │
│  └──────────────────────────────────────────────────┘   │
│                         │                                │
│  ┌──────────────────────┴───────────────────────────┐   │
│  │         ORDS REST API (9 endpoints)               │   │
│  │         Module: /olympics/                         │   │
│  └───────────────────────────────────────────────────┘   │
│                                                          │
│  ┌──────────────┐                                        │
│  │ DS4: CSV      │                                        │
│  │(FDBO.CSV_*)   │                                        │
│  │ 50,000 rows   │                                        │
│  └──────────────┘                                        │
└─────────────────────────────────────────────────────────┘
```

### 📊 Statistici Date

| Sursă | Schema | Tabele | Rânduri |
|-------|--------|--------|---------|
| DS1: Oracle | OLY_REF | SPORTS, EVENTS, GAMES, COUNTRIES | 1,113 |
| DS2: PostgreSQL | FDBO | PG_ATHLETES, PG_RESULTS | 406,687 |
| DS3: MongoDB/JSON | FDBO | JSON_MEDAL_DOCS (JSON_TABLE) | 28,302 |
| DS4: CSV | FDBO | CSV_ATHLETE_EVENTS | 50,000 |
| **TOTAL** | | | **486,102** |

### Relații între Tabele (FK)

```
athletes.athlete_id ←── results.athlete_id
games.game_id       ←── results.game_id
events.event_id     ←── results.event_id
sports.sport_id     ←── events.sport_id
countries.noc       ←── results.noc

JSON_MEDAL_DOCS.json_data → athlete medal/game summary  (doc_type)
CSV_ATHLETE_EVENTS → enriched denormalized events
```

### 🗂️ Structura Proiectului

```
Proiect/
├── 0. Data Preparation/
│   ├── prepare_data.py          # Normalizare dataset Kaggle → 4 formate
│   ├── setup_oracle_cloud.py    # Creare users + tabele pe Oracle Cloud
│   ├── load_to_oracle.py        # Încărcare date din CSV/SQL/JSONL
│   ├── create_views.py          # Creare toate view-urile + ORDS
│   ├── fix_views.py             # Fix JSON paths + aggregate
│   └── test_connection.py       # Test conexiune Oracle Cloud
│
├── 1. Data Sources/
│   ├── 00.SCHEMA.sql                    # Schema PostgreSQL
│   ├── 01.staging_pg.sql                # Staging PostgreSQL
│   ├── 02.copy_pg.sql                   # COPY CSV → PostgreSQL
│   ├── 03.create_pg.sql                 # CREATE tables PostgreSQL
│   ├── 04.insert_pg.sql                 # INSERT PostgreSQL (normalizat)
│   ├── CLOUD_01_CREATE_USERS.sql        # Creare OLY_REF + FDBO users
│   ├── CLOUD_02_OLY_REF_TABLES.sql      # Tabele referință Oracle
│   ├── CLOUD_03_OLY_REF_DATA.sql        # Date referință (INSERT)
│   ├── CLOUD_03b_OLY_REF_EVENTS.sql     # Events (primele 100)
│   └── CLOUD_04_FDBO_TABLES.sql         # Tabele FDBO (PG, JSON, CSV)
│
├── 2. Access Model/
│   └── CLOUD_06_FDBO_ACCESS_VIEWS.sql   # 9 federated views (JSON_TABLE)
│
├── 3. Integration Model/
│   ├── CLOUD_07_INTEGRATION_VIEWS.sql   # VIEW URIAȘ + Dim + Fact
│   ├── CLOUD_08_OLAP_QUERIES.sql        # 3 ROLLUP + 3 CUBE + 3 GS
│   └── CLOUD_09_WINDOW_FUNCTIONS.sql    # 8 window function views
│
├── 4. Web Model/
│   └── CLOUD_10_ORDS_REST_API.sql       # ORDS module + 9 endpoints
│
└── README.md
```

### 🔧 Configurare & Instalare

#### Cerințe
- Python 3.10+ cu `oracledb`, `pandas`
- PostgreSQL 16 (local)
- Oracle Cloud Autonomous Database (Free Tier)
- SQL Developer 24+

#### Pași de Execuție

1. Descarcă datasetul de pe Kaggle
2. Rulează `prepare_data.py` pentru a genera fișierele sursă (pg_data/, oracle_data/, mongo_data/, csv_data/)
3. PostgreSQL: Execută scripturile 00-04 în psql
4. Oracle Cloud: Rulează `setup_oracle_cloud.py` (creare users + tabele)
5. Oracle Cloud: Rulează `load_to_oracle.py` (încărcare toate datele)
6. Oracle Cloud: Rulează `create_views.py` + `fix_views.py` (views + ORDS)
7. Verifică endpoint-urile REST

### 📋 Lista View-urilor (32 total)

#### Access Views (9)
| View | Sursa | Descriere |
|------|-------|-----------|
| FDBO_SPORTS_V | DS1-Oracle | Sporturi olimpice |
| FDBO_EVENTS_V | DS1-Oracle | Evenimente cu sport name |
| FDBO_GAMES_V | DS1-Oracle | Ediții olimpice |
| FDBO_COUNTRIES_V | DS1-Oracle | Țări/NOC |
| FDBO_PG_ATHLETES_V | DS2-PostgreSQL | Atleți (135K) |
| FDBO_PG_RESULTS_V | DS2-PostgreSQL | Rezultate (271K) |
| FDBO_JSON_ATHLETE_MEDALS_V | DS3-JSON | Medalii per atlet (JSON_TABLE) |
| FDBO_JSON_GAME_SUMMARY_V | DS3-JSON | Sumar ediții (JSON_TABLE) |
| FDBO_CSV_EVENTS_V | DS4-CSV | Evenimente enriched |

#### Integration Views (6)
| View | Descriere |
|------|-----------|
| **INT_RESULTS_FULL_V** | **VIEW URIAȘ** — 271,116 rânduri, 22 coloane, JOIN peste DS1+DS2 |
| DIM_ATHLETE_V | Dimensiune atlet cu țară |
| DIM_GAME_V | Dimensiune ediție cu eră clasificată |
| DIM_EVENT_V | Dimensiune eveniment cu categorie gen |
| DIM_COUNTRY_V | Dimensiune țară cu continent |
| FACT_RESULTS_V | Tabel fapte cu indicatori medalie |

#### OLAP Views (9)
| View | Tip | Descriere |
|------|-----|-----------|
| OLAP_ROLLUP_COUNTRY_SPORT_V | ROLLUP | Medalii per țară→sport→total |
| OLAP_ROLLUP_SEASON_YEAR_V | ROLLUP | Medalii per sezon→an→oraș |
| OLAP_ROLLUP_SEX_AGE_V | ROLLUP | Medalii per sex→grupă vârstă |
| OLAP_CUBE_COUNTRY_SEASON_V | CUBE | Țară × Sezon (toate combinațiile) |
| OLAP_CUBE_SEX_MEDAL_V | CUBE | Sex × Tip medalie |
| OLAP_CUBE_SPORT_ERA_V | CUBE | Sport × Eră istorică |
| OLAP_GS_COUNTRY_SPORT_SEASON_V | GROUPING SETS | Grupări independente |
| OLAP_GS_MIXED_V | GROUPING SETS | Grupări mixte (țară+an, sport+sex) |
| OLAP_GS_DATA_SOURCES_V | GROUPING SETS | Statistici pe surse de date |

#### Window Function Views (8)
| View | Funcții | Descriere |
|------|---------|-----------|
| WF_TOP_MEDALISTS_V | ROW_NUMBER, RANK, DENSE_RANK | Top medaliști per țară |
| WF_RUNNING_MEDALS_V | SUM() OVER | Medalii cumulate per an |
| WF_MOVING_AVG_MEDALS_V | AVG() OVER (ROWS BETWEEN) | Medie mobilă 3/5 ediții |
| WF_LAG_LEAD_EDITIONS_V | LAG, LEAD | Comparație cu ediția anterioară |
| WF_ATHLETE_QUARTILES_V | NTILE, PERCENT_RANK, CUME_DIST | Quartile medaliști |
| WF_FIRST_LAST_MEDAL_V | FIRST_VALUE, LAST_VALUE | Prima/ultima medalie per sport |
| WF_MEDAL_SHARE_V | RATIO_TO_REPORT, RANK | Procentul medalii per țară |
| WF_COUNTRY_TOP_SPORTS_V | LISTAGG + ROW_NUMBER | Top sporturi per țară |

### 🌐 ORDS REST API Endpoints

Module: `olympics` | Base: `/fdbo/olympics/`

| Endpoint | Descriere | Sursa |
|----------|-----------|-------|
| GET /sports | Lista sporturilor | DS1-Oracle |
| GET /games | Lista edițiilor | DS1-Oracle |
| GET /countries | Lista țărilor | DS1-Oracle |
| GET /top-medalists | Top 50 medaliști | Window Functions |
| GET /medal-share | Top 50 țări (% medalii) | Window Functions |
| GET /olap/country-medals | Top 30 țări cu subtotaluri | ROLLUP |
| GET /data-sources | Statistici surse date | GROUPING SETS |
| GET /json/athlete-medals | Medalii atleți (JSON_TABLE) | DS3-JSON |
| GET /growth | Evoluție olimpiadă (LAG/LEAD) | Window Functions |

### 🏆 Rezultate Notabile

- **Michael Phelps**: 28 medalii (23 🥇, 3 🥈, 2 🥉) — #1 global
- **USA**: 5,637 medalii (2,638 🥇) — 14.14% din total
- **România**: 653 medalii (161 🥇) — Rank #17 global, 1.64% din total
- **Grand Total**: 486,102 înregistrări din 4 surse de date

### ⚙️ Tehnologii
- **Oracle Cloud Autonomous Database 26ai** (Always Free Tier)
- **PostgreSQL 16** (local)
- **Python 3.12** (oracledb, pandas)
- **Oracle JSON_TABLE** (parsare documente JSON/MongoDB)
- **ORDS REST API** (endpoints automate)
- **SQL Developer 24.3.1** (administrare)

### Surse de date (4)
| DS | Tehnologie | Ce conține | Cum e accesat din FDBO |
|----|------------|------------|------------------------|
| DS1 | Oracle (OLY_REF) | games, sports, events, countries | GRANT SELECT cross-schema |
| DS2 | PostgreSQL (simulat) | athletes (135K), results (271K) | Tabele FDBO.PG_* |
| DS3 | MongoDB/JSON | athlete_medals_docs, game_summary_docs | FDBO.JSON_MEDAL_DOCS + JSON_TABLE |
| DS4 | CSV | athlete_events_enriched (50K) | FDBO.CSV_ATHLETE_EVENTS |

---
*Dataset: [120 Years of Olympic History](https://www.kaggle.com/datasets/heesoo37/120-years-of-olympic-history-athletes-and-results) — Kaggle*
