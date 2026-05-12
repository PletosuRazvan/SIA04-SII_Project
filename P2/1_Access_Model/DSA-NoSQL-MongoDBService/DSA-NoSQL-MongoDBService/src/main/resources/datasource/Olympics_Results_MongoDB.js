// ============================================================
// MongoDB: Import Olympics Results data
// Run in mongosh: mongosh < Olympics_Results_MongoDB.js
// Or import the JSON: mongoimport --db olympics --collection results --jsonArray --file mongo_results.json
// ============================================================

// Switch to olympics database
use('olympics');

// Drop existing collection if needed
db.results.drop();

// Insert sample results data (matches athletes from athletes.csv)
db.results.insertMany([
    {"Athlete_ID": 1, "Games": "1992 Summer", "NOC": "CHN", "Sport": "Basketball", "Event": "Basketball Men's Basketball", "Medal": "NA", "Age": 24},
    {"Athlete_ID": 2, "Games": "2012 Summer", "NOC": "CHN", "Sport": "Judo", "Event": "Judo Men's Extra-Lightweight", "Medal": "NA", "Age": 23},
    {"Athlete_ID": 11, "Games": "2000 Summer", "NOC": "USA", "Sport": "Swimming", "Event": "Swimming Men's 200 metres Butterfly", "Medal": "Gold", "Age": 15},
    {"Athlete_ID": 11, "Games": "2004 Summer", "NOC": "USA", "Sport": "Swimming", "Event": "Swimming Men's 200 metres Butterfly", "Medal": "Gold", "Age": 19},
    {"Athlete_ID": 11, "Games": "2004 Summer", "NOC": "USA", "Sport": "Swimming", "Event": "Swimming Men's 100 metres Butterfly", "Medal": "Gold", "Age": 19},
    {"Athlete_ID": 11, "Games": "2004 Summer", "NOC": "USA", "Sport": "Swimming", "Event": "Swimming Men's 200 metres Individual Medley", "Medal": "Gold", "Age": 19},
    {"Athlete_ID": 11, "Games": "2004 Summer", "NOC": "USA", "Sport": "Swimming", "Event": "Swimming Men's 400 metres Individual Medley", "Medal": "Gold", "Age": 19},
    {"Athlete_ID": 11, "Games": "2008 Summer", "NOC": "USA", "Sport": "Swimming", "Event": "Swimming Men's 100 metres Butterfly", "Medal": "Gold", "Age": 23},
    {"Athlete_ID": 11, "Games": "2008 Summer", "NOC": "USA", "Sport": "Swimming", "Event": "Swimming Men's 200 metres Butterfly", "Medal": "Gold", "Age": 23},
    {"Athlete_ID": 11, "Games": "2008 Summer", "NOC": "USA", "Sport": "Swimming", "Event": "Swimming Men's 200 metres Freestyle", "Medal": "Gold", "Age": 23},
    {"Athlete_ID": 11, "Games": "2008 Summer", "NOC": "USA", "Sport": "Swimming", "Event": "Swimming Men's 400 metres Individual Medley", "Medal": "Gold", "Age": 23},
    {"Athlete_ID": 11, "Games": "2012 Summer", "NOC": "USA", "Sport": "Swimming", "Event": "Swimming Men's 100 metres Butterfly", "Medal": "Gold", "Age": 27},
    {"Athlete_ID": 11, "Games": "2012 Summer", "NOC": "USA", "Sport": "Swimming", "Event": "Swimming Men's 200 metres Individual Medley", "Medal": "Gold", "Age": 27},
    {"Athlete_ID": 11, "Games": "2016 Summer", "NOC": "USA", "Sport": "Swimming", "Event": "Swimming Men's 200 metres Individual Medley", "Medal": "Gold", "Age": 31},
    {"Athlete_ID": 12, "Games": "2008 Summer", "NOC": "JAM", "Sport": "Athletics", "Event": "Athletics Men's 100 metres", "Medal": "Gold", "Age": 22},
    {"Athlete_ID": 12, "Games": "2008 Summer", "NOC": "JAM", "Sport": "Athletics", "Event": "Athletics Men's 200 metres", "Medal": "Gold", "Age": 22},
    {"Athlete_ID": 12, "Games": "2012 Summer", "NOC": "JAM", "Sport": "Athletics", "Event": "Athletics Men's 100 metres", "Medal": "Gold", "Age": 26},
    {"Athlete_ID": 12, "Games": "2012 Summer", "NOC": "JAM", "Sport": "Athletics", "Event": "Athletics Men's 200 metres", "Medal": "Gold", "Age": 26},
    {"Athlete_ID": 12, "Games": "2016 Summer", "NOC": "JAM", "Sport": "Athletics", "Event": "Athletics Men's 100 metres", "Medal": "Gold", "Age": 30},
    {"Athlete_ID": 13, "Games": "2016 Summer", "NOC": "USA", "Sport": "Gymnastics", "Event": "Gymnastics Women's Individual All-Around", "Medal": "Gold", "Age": 19},
    {"Athlete_ID": 13, "Games": "2016 Summer", "NOC": "USA", "Sport": "Gymnastics", "Event": "Gymnastics Women's Floor Exercise", "Medal": "Gold", "Age": 19},
    {"Athlete_ID": 13, "Games": "2016 Summer", "NOC": "USA", "Sport": "Gymnastics", "Event": "Gymnastics Women's Balance Beam", "Medal": "Bronze", "Age": 19},
    {"Athlete_ID": 13, "Games": "2016 Summer", "NOC": "USA", "Sport": "Gymnastics", "Event": "Gymnastics Women's Team All-Around", "Medal": "Gold", "Age": 19},
    {"Athlete_ID": 14, "Games": "1976 Summer", "NOC": "ROU", "Sport": "Gymnastics", "Event": "Gymnastics Women's Individual All-Around", "Medal": "Gold", "Age": 14},
    {"Athlete_ID": 14, "Games": "1976 Summer", "NOC": "ROU", "Sport": "Gymnastics", "Event": "Gymnastics Women's Uneven Bars", "Medal": "Gold", "Age": 14},
    {"Athlete_ID": 14, "Games": "1976 Summer", "NOC": "ROU", "Sport": "Gymnastics", "Event": "Gymnastics Women's Balance Beam", "Medal": "Gold", "Age": 14},
    {"Athlete_ID": 14, "Games": "1980 Summer", "NOC": "ROU", "Sport": "Gymnastics", "Event": "Gymnastics Women's Balance Beam", "Medal": "Gold", "Age": 18},
    {"Athlete_ID": 14, "Games": "1980 Summer", "NOC": "ROU", "Sport": "Gymnastics", "Event": "Gymnastics Women's Floor Exercise", "Medal": "Gold", "Age": 18},
    {"Athlete_ID": 15, "Games": "1984 Summer", "NOC": "USA", "Sport": "Athletics", "Event": "Athletics Men's 100 metres", "Medal": "Gold", "Age": 23},
    {"Athlete_ID": 15, "Games": "1984 Summer", "NOC": "USA", "Sport": "Athletics", "Event": "Athletics Men's 200 metres", "Medal": "Gold", "Age": 23},
    {"Athlete_ID": 15, "Games": "1984 Summer", "NOC": "USA", "Sport": "Athletics", "Event": "Athletics Men's Long Jump", "Medal": "Gold", "Age": 23},
    {"Athlete_ID": 15, "Games": "1988 Summer", "NOC": "USA", "Sport": "Athletics", "Event": "Athletics Men's 100 metres", "Medal": "Gold", "Age": 27},
    {"Athlete_ID": 15, "Games": "1988 Summer", "NOC": "USA", "Sport": "Athletics", "Event": "Athletics Men's Long Jump", "Medal": "Gold", "Age": 27},
    {"Athlete_ID": 16, "Games": "1956 Summer", "NOC": "URS", "Sport": "Gymnastics", "Event": "Gymnastics Women's Individual All-Around", "Medal": "Gold", "Age": 21},
    {"Athlete_ID": 16, "Games": "1960 Summer", "NOC": "URS", "Sport": "Gymnastics", "Event": "Gymnastics Women's Individual All-Around", "Medal": "Gold", "Age": 25},
    {"Athlete_ID": 16, "Games": "1964 Summer", "NOC": "URS", "Sport": "Gymnastics", "Event": "Gymnastics Women's Floor Exercise", "Medal": "Gold", "Age": 29},
    {"Athlete_ID": 17, "Games": "1920 Summer", "NOC": "FIN", "Sport": "Athletics", "Event": "Athletics Men's 10000 metres", "Medal": "Gold", "Age": 23},
    {"Athlete_ID": 17, "Games": "1924 Summer", "NOC": "FIN", "Sport": "Athletics", "Event": "Athletics Men's 1500 metres", "Medal": "Gold", "Age": 27},
    {"Athlete_ID": 17, "Games": "1924 Summer", "NOC": "FIN", "Sport": "Athletics", "Event": "Athletics Men's 5000 metres", "Medal": "Gold", "Age": 27},
    {"Athlete_ID": 17, "Games": "1928 Summer", "NOC": "FIN", "Sport": "Athletics", "Event": "Athletics Men's 10000 metres", "Medal": "Gold", "Age": 31},
    {"Athlete_ID": 18, "Games": "1972 Summer", "NOC": "USA", "Sport": "Swimming", "Event": "Swimming Men's 100 metres Freestyle", "Medal": "Gold", "Age": 22},
    {"Athlete_ID": 18, "Games": "1972 Summer", "NOC": "USA", "Sport": "Swimming", "Event": "Swimming Men's 200 metres Butterfly", "Medal": "Gold", "Age": 22},
    {"Athlete_ID": 18, "Games": "1972 Summer", "NOC": "USA", "Sport": "Swimming", "Event": "Swimming Men's 100 metres Butterfly", "Medal": "Gold", "Age": 22},
    {"Athlete_ID": 20, "Games": "1936 Summer", "NOC": "USA", "Sport": "Athletics", "Event": "Athletics Men's 100 metres", "Medal": "Gold", "Age": 23},
    {"Athlete_ID": 20, "Games": "1936 Summer", "NOC": "USA", "Sport": "Athletics", "Event": "Athletics Men's 200 metres", "Medal": "Gold", "Age": 23},
    {"Athlete_ID": 20, "Games": "1936 Summer", "NOC": "USA", "Sport": "Athletics", "Event": "Athletics Men's Long Jump", "Medal": "Gold", "Age": 23},
    {"Athlete_ID": 20, "Games": "1936 Summer", "NOC": "USA", "Sport": "Athletics", "Event": "Athletics Men's 4 x 100 metres Relay", "Medal": "Gold", "Age": 23},
    {"Athlete_ID": 22, "Games": "1932 Summer", "NOC": "HUN", "Sport": "Fencing", "Event": "Fencing Men's Sabre Individual", "Medal": "Gold", "Age": 22},
    {"Athlete_ID": 22, "Games": "1936 Summer", "NOC": "HUN", "Sport": "Fencing", "Event": "Fencing Men's Sabre Team", "Medal": "Gold", "Age": 26},
    {"Athlete_ID": 22, "Games": "1948 Summer", "NOC": "HUN", "Sport": "Fencing", "Event": "Fencing Men's Sabre Individual", "Medal": "Gold", "Age": 38},
    {"Athlete_ID": 22, "Games": "1952 Summer", "NOC": "HUN", "Sport": "Fencing", "Event": "Fencing Men's Sabre Team", "Medal": "Gold", "Age": 42},
    {"Athlete_ID": 22, "Games": "1956 Summer", "NOC": "HUN", "Sport": "Fencing", "Event": "Fencing Men's Sabre Team", "Medal": "Gold", "Age": 46},
    {"Athlete_ID": 22, "Games": "1960 Summer", "NOC": "HUN", "Sport": "Fencing", "Event": "Fencing Men's Sabre Team", "Medal": "Gold", "Age": 50},
    {"Athlete_ID": 23, "Games": "1980 Summer", "NOC": "GER", "Sport": "Canoeing", "Event": "Canoeing Women's Kayak Singles 500 metres", "Medal": "Gold", "Age": 18},
    {"Athlete_ID": 23, "Games": "1988 Summer", "NOC": "GER", "Sport": "Canoeing", "Event": "Canoeing Women's Kayak Singles 500 metres", "Medal": "Silver", "Age": 26},
    {"Athlete_ID": 23, "Games": "1992 Summer", "NOC": "GER", "Sport": "Canoeing", "Event": "Canoeing Women's Kayak Singles 500 metres", "Medal": "Gold", "Age": 30},
    {"Athlete_ID": 23, "Games": "1996 Summer", "NOC": "GER", "Sport": "Canoeing", "Event": "Canoeing Women's Kayak Doubles 500 metres", "Medal": "Gold", "Age": 34},
    {"Athlete_ID": 23, "Games": "2000 Summer", "NOC": "GER", "Sport": "Canoeing", "Event": "Canoeing Women's Kayak Fours 500 metres", "Medal": "Gold", "Age": 38},
    {"Athlete_ID": 23, "Games": "2004 Summer", "NOC": "GER", "Sport": "Canoeing", "Event": "Canoeing Women's Kayak Fours 500 metres", "Medal": "Gold", "Age": 42},
    {"Athlete_ID": 24, "Games": "1968 Summer", "NOC": "JPN", "Sport": "Gymnastics", "Event": "Gymnastics Men's Individual All-Around", "Medal": "Gold", "Age": 22},
    {"Athlete_ID": 24, "Games": "1972 Summer", "NOC": "JPN", "Sport": "Gymnastics", "Event": "Gymnastics Men's Individual All-Around", "Medal": "Gold", "Age": 26},
    {"Athlete_ID": 24, "Games": "1976 Summer", "NOC": "JPN", "Sport": "Gymnastics", "Event": "Gymnastics Men's Parallel Bars", "Medal": "Gold", "Age": 30},
    {"Athlete_ID": 29, "Games": "1972 Summer", "NOC": "URS", "Sport": "Gymnastics", "Event": "Gymnastics Men's Floor Exercise", "Medal": "Gold", "Age": 20},
    {"Athlete_ID": 29, "Games": "1976 Summer", "NOC": "URS", "Sport": "Gymnastics", "Event": "Gymnastics Men's Individual All-Around", "Medal": "Gold", "Age": 24},
    {"Athlete_ID": 29, "Games": "1976 Summer", "NOC": "URS", "Sport": "Gymnastics", "Event": "Gymnastics Men's Rings", "Medal": "Gold", "Age": 24},
    {"Athlete_ID": 29, "Games": "1980 Summer", "NOC": "URS", "Sport": "Gymnastics", "Event": "Gymnastics Men's Vault", "Medal": "Gold", "Age": 28},
    {"Athlete_ID": 30, "Games": "1900 Summer", "NOC": "USA", "Sport": "Athletics", "Event": "Athletics Men's Standing High Jump", "Medal": "Gold", "Age": 27},
    {"Athlete_ID": 30, "Games": "1900 Summer", "NOC": "USA", "Sport": "Athletics", "Event": "Athletics Men's Standing Long Jump", "Medal": "Gold", "Age": 27},
    {"Athlete_ID": 30, "Games": "1904 Summer", "NOC": "USA", "Sport": "Athletics", "Event": "Athletics Men's Standing High Jump", "Medal": "Gold", "Age": 31},
    {"Athlete_ID": 30, "Games": "1904 Summer", "NOC": "USA", "Sport": "Athletics", "Event": "Athletics Men's Standing Long Jump", "Medal": "Gold", "Age": 31},
    {"Athlete_ID": 30, "Games": "1908 Summer", "NOC": "USA", "Sport": "Athletics", "Event": "Athletics Men's Standing High Jump", "Medal": "Gold", "Age": 35},
    {"Athlete_ID": 30, "Games": "1908 Summer", "NOC": "USA", "Sport": "Athletics", "Event": "Athletics Men's Standing Long Jump", "Medal": "Gold", "Age": 35}
]);

print("Inserted " + db.results.countDocuments() + " results into olympics.results collection");

