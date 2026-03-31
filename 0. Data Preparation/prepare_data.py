"""
Sistem Integrat de Analiză a Datelor Olimpice
==============================================
Script de pregătire date - normalizare și distribuire pe surse

Kaggle Dataset: 120 Years of Olympic History
https://www.kaggle.com/datasets/heesoo37/120-years-of-olympic-history-athletes-and-results

Descarcă cele 2 fișiere CSV și pune-le în același folder cu acest script:
  - athlete_events.csv
  - noc_regions.csv

Instalare dependințe:
  pip install pandas

Apoi rulează:
  python prepare_data.py

Output-uri:
  pg_data/          → CSV-uri pentru PostgreSQL
  oracle_data/      → INSERT SQL pentru Oracle OLY_REF
  mongo_data/       → JSONL pentru MongoDB
  csv_data/         → CSV mare (athlete_events_enriched.csv) ca sursă DS4
"""

import pandas as pd
import json
import os

def main():
    print("=" * 60)
    print("  PREGĂTIRE DATE - JOCURI OLIMPICE")
    print("=" * 60)

    print("\n📖 Citire date brute...")
    df = pd.read_csv('athlete_events.csv')
    noc_df = pd.read_csv('noc_regions.csv')
    print(f"   athlete_events.csv: {len(df):,} rânduri")
    print(f"   noc_regions.csv:    {len(noc_df):,} rânduri")

    for d in ['pg_data', 'oracle_data', 'mongo_data', 'csv_data']:
        os.makedirs(d, exist_ok=True)

    print("\n🔧 Normalizare date...")

    sports = (df[['Sport']]
              .drop_duplicates()
              .sort_values('Sport')
              .reset_index(drop=True))
    sports.index += 1
    sports = sports.reset_index()
    sports.columns = ['sport_id', 'sport_name']
    sport_map = dict(zip(sports['sport_name'], sports['sport_id']))
    print(f"   sports:    {len(sports):>6} rânduri")

    events = (df[['Event', 'Sport']]
              .drop_duplicates()
              .sort_values(['Sport', 'Event'])
              .reset_index(drop=True))
    events.index += 1
    events = events.reset_index()
    events.columns = ['event_id', 'event_name', 'sport_name']
    events['sport_id'] = events['sport_name'].map(sport_map)
    event_map = dict(zip(events['event_name'], events['event_id']))
    events = events[['event_id', 'sport_id', 'event_name']]
    print(f"   events:    {len(events):>6} rânduri")

    games = (df[['Games', 'Year', 'Season', 'City']]
             .drop_duplicates()
             .sort_values('Year')
             .reset_index(drop=True))
    games.index += 1
    games = games.reset_index()
    games.columns = ['game_id', 'games_name', 'year', 'season', 'city']
    game_map = dict(zip(games['games_name'], games['game_id']))
    print(f"   games:     {len(games):>6} rânduri")

    countries = noc_df.copy()
    countries.columns = ['noc', 'region', 'notes']
    countries['notes'] = countries['notes'].fillna('')
    print(f"   countries: {len(countries):>6} rânduri")

    athletes = (df[['ID', 'Name', 'Sex']]
                .drop_duplicates(subset=['ID'])
                .sort_values('ID')
                .reset_index(drop=True))
    hw = df.groupby('ID').agg({'Height': 'mean', 'Weight': 'mean'}).reset_index()
    athletes = athletes.merge(hw, on='ID', how='left')
    athletes.columns = ['athlete_id', 'name', 'sex', 'height', 'weight']
    print(f"   athletes:  {len(athletes):>6} rânduri")

    results = df.copy()
    results['game_id'] = results['Games'].map(game_map)
    results['event_id'] = results['Event'].map(event_map)
    results = results[['ID', 'game_id', 'event_id', 'NOC', 'Team', 'Age', 'Medal']]
    results = results.reset_index(drop=True)
    results.index += 1
    results = results.reset_index()
    results.columns = ['result_id', 'athlete_id', 'game_id', 'event_id',
                        'noc', 'team', 'age', 'medal']
    results['medal'] = results['medal'].fillna('None')
    print(f"   results:   {len(results):>6} rânduri")

    print("\n📁 DS1 - Generare CSV-uri pentru PostgreSQL...")
    athletes.to_csv('pg_data/athletes.csv', index=False)
    results.to_csv('pg_data/results.csv', index=False)
    print("   ✅ pg_data/athletes.csv")
    print("   ✅ pg_data/results.csv")

    print("\n📁 DS2 - Generare INSERT SQL pentru Oracle OLY_REF...")

    with open('oracle_data/insert_games.sql', 'w', encoding='utf-8') as f:
        f.write("-- INSERT GAMES into OLY_REF\n")
        f.write("-- Rulează ca OLY_REF pe XEPDB1\n\n")
        for _, row in games.iterrows():
            city = str(row['city']).replace("'", "''")
            gname = str(row['games_name']).replace("'", "''")
            f.write(f"INSERT INTO OLY_REF.GAMES (game_id, games_name, year, season, city) "
                    f"VALUES ({row['game_id']}, '{gname}', {row['year']}, "
                    f"'{row['season']}', '{city}');\n")
        f.write("\nCOMMIT;\n")
    print(f"   ✅ oracle_data/insert_games.sql ({len(games)} rows)")

    with open('oracle_data/insert_sports.sql', 'w', encoding='utf-8') as f:
        f.write("-- INSERT SPORTS into OLY_REF\n")
        f.write("-- Rulează ca OLY_REF pe XEPDB1\n\n")
        for _, row in sports.iterrows():
            sname = str(row['sport_name']).replace("'", "''")
            f.write(f"INSERT INTO OLY_REF.SPORTS (sport_id, sport_name) "
                    f"VALUES ({row['sport_id']}, '{sname}');\n")
        f.write("\nCOMMIT;\n")
    print(f"   ✅ oracle_data/insert_sports.sql ({len(sports)} rows)")

    with open('oracle_data/insert_events.sql', 'w', encoding='utf-8') as f:
        f.write("-- INSERT EVENTS into OLY_REF\n")
        f.write("-- Rulează ca OLY_REF pe XEPDB1\n\n")
        for _, row in events.iterrows():
            ename = str(row['event_name']).replace("'", "''")
            f.write(f"INSERT INTO OLY_REF.EVENTS (event_id, sport_id, event_name) "
                    f"VALUES ({row['event_id']}, {row['sport_id']}, '{ename}');\n")
        f.write("\nCOMMIT;\n")
    print(f"   ✅ oracle_data/insert_events.sql ({len(events)} rows)")

    with open('oracle_data/insert_countries.sql', 'w', encoding='utf-8') as f:
        f.write("-- INSERT COUNTRIES into OLY_REF\n")
        f.write("-- Rulează ca OLY_REF pe XEPDB1\n\n")
        for _, row in countries.iterrows():
            region = str(row['region']).replace("'", "''")
            notes = str(row['notes']).replace("'", "''")
            f.write(f"INSERT INTO OLY_REF.COUNTRIES (noc, region, notes) "
                    f"VALUES ('{row['noc']}', '{region}', '{notes}');\n")
        f.write("\nCOMMIT;\n")
    print(f"   ✅ oracle_data/insert_countries.sql ({len(countries)} rows)")

    print("\n📁 DS3 - Generare JSONL pentru MongoDB...")

    medalists = results[results['medal'] != 'None'].copy()
    athlete_medals = medalists.groupby('athlete_id').agg(
        total_medals=('medal', 'count'),
        gold=('medal', lambda x: (x == 'Gold').sum()),
        silver=('medal', lambda x: (x == 'Silver').sum()),
        bronze=('medal', lambda x: (x == 'Bronze').sum()),
        sports_count=('event_id', 'nunique'),
        games_count=('game_id', 'nunique'),
        first_game_id=('game_id', 'min'),
        last_game_id=('game_id', 'max')
    ).reset_index()

    athlete_names = dict(zip(athletes['athlete_id'], athletes['name']))
    athlete_sex = dict(zip(athletes['athlete_id'], athletes['sex']))
    athlete_medals['name'] = athlete_medals['athlete_id'].map(athlete_names)
    athlete_medals['sex'] = athlete_medals['athlete_id'].map(athlete_sex)

    with open('mongo_data/athlete_medals_docs.jsonl', 'w', encoding='utf-8') as f:
        for _, row in athlete_medals.iterrows():
            doc = {
                'athlete_id': int(row['athlete_id']),
                'name': row['name'],
                'sex': row['sex'],
                'total_medals': int(row['total_medals']),
                'gold': int(row['gold']),
                'silver': int(row['silver']),
                'bronze': int(row['bronze']),
                'sports_count': int(row['sports_count']),
                'games_count': int(row['games_count']),
                'first_game_id': int(row['first_game_id']),
                'last_game_id': int(row['last_game_id'])
            }
            f.write(json.dumps(doc, ensure_ascii=False) + '\n')
    print(f"   ✅ mongo_data/athlete_medals_docs.jsonl ({len(athlete_medals):,} docs)")

    game_summary = results.groupby('game_id').agg(
        total_athletes=('athlete_id', 'nunique'),
        total_entries=('result_id', 'count'),
        total_medals=('medal', lambda x: (x != 'None').sum()),
        gold_medals=('medal', lambda x: (x == 'Gold').sum()),
        silver_medals=('medal', lambda x: (x == 'Silver').sum()),
        bronze_medals=('medal', lambda x: (x == 'Bronze').sum()),
        countries_count=('noc', 'nunique'),
        events_count=('event_id', 'nunique')
    ).reset_index()

    game_details = {}
    for _, row in games.iterrows():
        game_details[row['game_id']] = {
            'games_name': row['games_name'],
            'year': int(row['year']),
            'season': row['season'],
            'city': row['city']
        }

    with open('mongo_data/game_summary_docs.jsonl', 'w', encoding='utf-8') as f:
        for _, row in game_summary.iterrows():
            gd = game_details.get(row['game_id'], {})
            doc = {
                'game_id': int(row['game_id']),
                'games_name': gd.get('games_name', ''),
                'year': gd.get('year', 0),
                'season': gd.get('season', ''),
                'city': gd.get('city', ''),
                'total_athletes': int(row['total_athletes']),
                'total_entries': int(row['total_entries']),
                'medals': {
                    'total': int(row['total_medals']),
                    'gold': int(row['gold_medals']),
                    'silver': int(row['silver_medals']),
                    'bronze': int(row['bronze_medals'])
                },
                'countries_count': int(row['countries_count']),
                'events_count': int(row['events_count'])
            }
            f.write(json.dumps(doc, ensure_ascii=False) + '\n')
    print(f"   ✅ mongo_data/game_summary_docs.jsonl ({len(game_summary):,} docs)")

    print("\n📁 DS4 - Generare CSV mare îmbogățit (sursă CSV directă)...")

    enriched = df.copy()
    enriched['game_id'] = enriched['Games'].map(game_map)
    enriched['event_id'] = enriched['Event'].map(event_map)
    enriched['sport_id'] = enriched['Sport'].map(sport_map)
    enriched = enriched.merge(noc_df[['NOC', 'region']], on='NOC', how='left')
    enriched = enriched.rename(columns={'region': 'country'})
    enriched['Medal'] = enriched['Medal'].fillna('None')

    enriched_out = enriched[[
        'ID', 'Name', 'Sex', 'Age', 'Height', 'Weight',
        'Team', 'NOC', 'country',
        'Games', 'Year', 'Season', 'City',
        'Sport', 'Event',
        'Medal',
        'game_id', 'event_id', 'sport_id'
    ]].copy()
    enriched_out.columns = [
        'athlete_id', 'name', 'sex', 'age', 'height', 'weight',
        'team', 'noc', 'country',
        'games_name', 'year', 'season', 'city',
        'sport_name', 'event_name',
        'medal',
        'game_id', 'event_id', 'sport_id'
    ]

    enriched_out.to_csv('csv_data/athlete_events_enriched.csv', index=False)
    print(f"   ✅ csv_data/athlete_events_enriched.csv ({len(enriched_out):,} rânduri)")

    print("\n" + "=" * 60)
    print("  ✅ PREGĂTIREA DATELOR S-A FINALIZAT CU SUCCES!")
    print("=" * 60)
    print(f"""
Fișiere generate:
  pg_data/
    athletes.csv                ({len(athletes):>7,} rânduri)
    results.csv                 ({len(results):>7,} rânduri)
  oracle_data/
    insert_games.sql            ({len(games):>5} INSERT)
    insert_sports.sql           ({len(sports):>5} INSERT)
    insert_events.sql           ({len(events):>5} INSERT)
    insert_countries.sql        ({len(countries):>5} INSERT)
  mongo_data/
    athlete_medals_docs.jsonl   ({len(athlete_medals):>7,} documente)
    game_summary_docs.jsonl     ({len(game_summary):>7,} documente)
  csv_data/
    athlete_events_enriched.csv ({len(enriched_out):>7,} rânduri) ← CSV MARE

Copiază csv_data/ în C:\\fdbo_data\\

Pașii următori:
  1. Copiază pg_data/ și csv_data/ în C:\\fdbo_data\\
  2. Importă CSV-urile în PostgreSQL   (vezi 02.copy_pg.txt)
  3. Rulează insert_*.sql în Oracle    (ca OLY_REF pe XEPDB1)
  4. Importă JSONL-urile în MongoDB    (vezi 08.CMD_MONGO.txt)
  5. Execută scripturile SQL 00 → 20 din proiect
    """)

if __name__ == '__main__':
    main()
