import pandas as pd
import numpy as np

print("Încărcăm datele...")
events = pd.read_csv('athlete_events.csv')
regions = pd.read_csv('noc_regions.csv')

events = events.replace({np.nan: None})
regions = regions.replace({np.nan: None})

print("Generăm datele pentru Oracle...")
athletes = events[['ID', 'Name', 'Sex', 'Height', 'Weight']].drop_duplicates(subset='ID')
athletes.to_csv('oracle_athletes.csv', index=False)

print("Generăm datele pentru Postgres...")
regions.to_csv('postgres_regions.csv', index=False)

print("Generăm sursa CSV...")
games = events[['Games', 'Year', 'Season', 'City']].drop_duplicates(subset='Games')
games.to_csv('source_games.csv', index=False)

print("Generăm fișierul JSON pentru MongoDB și Oracle...")
results = events[['ID', 'Games', 'NOC', 'Sport', 'Event', 'Medal', 'Age']]
results = results.rename(columns={'ID': 'Athlete_ID'})

results.to_json('mongo_results.json', orient='records', indent=4)

print("Gata! Ai 4 fișiere noi pregătite pentru baze de date.")

rezultate = pd.read_json('mongo_results.json')

rezultate.to_json('oracle_results.json', orient='records', lines=True)
print("Gata JSON-ul pentru Oracle!")