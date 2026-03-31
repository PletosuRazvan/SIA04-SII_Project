-- Q1. Câte rânduri are VIEW-ul?
SELECT COUNT(*) AS total_rows FROM FDBO.INT_RESULTS_FULL_V;

-- Q2. Sample din VIEW-ul(22 coloane, date din DS1+DS2)
SELECT * FROM FDBO.INT_RESULTS_FULL_V WHERE ROWNUM <= 10;

-- Q3. Lista tuturor view-urilor din FDBO (32 total)
SELECT view_name FROM all_views WHERE owner = 'FDBO' ORDER BY view_name;

-- Q4. Câte rânduri are fiecare tabel? (GROUPING SETS data sources)
SELECT source_name, category, record_count, grouping_id 
FROM FDBO.OLAP_GS_DATA_SOURCES_V 
ORDER BY grouping_id, source_name;

-- Q5. JSON_TABLE - Top 10 medaliști din sursa MongoDB/JSON
SELECT athlete_name, total_medals, gold_medals, silver_medals, bronze_medals
FROM FDBO.FDBO_JSON_ATHLETE_MEDALS_V 
ORDER BY total_medals DESC 
FETCH FIRST 10 ROWS ONLY;

-- Q6. JSON_TABLE - Sumar ediții olimpice din JSON
SELECT games_name, year, total_athletes, countries_count, events_count
FROM FDBO.FDBO_JSON_GAME_SUMMARY_V 
ORDER BY year;

-- Q7. ROLLUP - Top 10 țări cu subtotaluri per sport
SELECT country_name, sport_name, total_medals, gold, silver, bronze,
       is_country_total, is_sport_total
FROM FDBO.OLAP_ROLLUP_COUNTRY_SPORT_V 
WHERE is_sport_total = 1 AND is_country_total = 0
ORDER BY total_medals DESC
FETCH FIRST 10 ROWS ONLY;

-- Q8. ROLLUP - Medalii per sezon, an, oraș (3 nivele de subtotal)
SELECT season, year, city, unique_athletes, total_medals,
       is_season_total, is_year_total, is_city_total
FROM FDBO.OLAP_ROLLUP_SEASON_YEAR_V
WHERE is_season_total = 0
ORDER BY season, year;

-- Q9. ROLLUP - Medalii per sex și grupă de vârstă
SELECT sex, age_group, participations, total_medals, gold, avg_age
FROM FDBO.OLAP_ROLLUP_SEX_AGE_V;

-- Q10. CUBE - Țări × Sezon (toate combinațiile)
SELECT country_name, season, total_medals, gold
FROM FDBO.OLAP_CUBE_COUNTRY_SEASON_V
WHERE country_name = 'Romania' OR is_country_total = 1
ORDER BY is_country_total, is_season_total;

-- Q11. CUBE - Sex × Tip medalie (totale pe fiecare dimensiune)
SELECT sex, medal_type, medal_count, unique_medalists, countries_count
FROM FDBO.OLAP_CUBE_SEX_MEDAL_V;

-- Q12. CUBE - Sport × Eră istorică
SELECT sport_name, era, participations, medals, countries
FROM FDBO.OLAP_CUBE_SPORT_ERA_V
WHERE sport_name = 'Gymnastics' OR is_sport_total = 1
ORDER BY is_sport_total, era;

-- Q13. GROUPING SETS - Grupări independente (țară SAU sport SAU sezon)
SELECT country_name, sport_name, season, medals, grouping_id
FROM FDBO.OLAP_GS_COUNTRY_SPORT_SEASON_V
WHERE country_name = 'Romania' 
   OR sport_name = 'Gymnastics'
   OR (season IS NOT NULL AND grouping_id = 6)
ORDER BY grouping_id;

-- Q14. Window: Top 20 medaliști global (ROW_NUMBER, RANK, DENSE_RANK)
SELECT athlete_name, country_name, total_medals, gold, global_dense_rank
FROM FDBO.WF_TOP_MEDALISTS_V 
WHERE global_dense_rank <= 20
ORDER BY global_dense_rank;

-- Q15. Window: Medalii cumulate de-a lungul anilor (SUM OVER)
SELECT year, season, medals_this_edition, cumulative_medals, 
       athletes_this_edition, cumulative_athletes
FROM FDBO.WF_RUNNING_MEDALS_V 
ORDER BY year;

-- Q16. Window: Medie mobilă 3/5 ediții (AVG OVER ROWS BETWEEN)
SELECT year, season, city, medal_count, 
       moving_avg_3, moving_avg_5, min_window_3, max_window_3
FROM FDBO.WF_MOVING_AVG_MEDALS_V 
ORDER BY year;

-- Q17. Window: Creștere față de ediția anterioară (LAG/LEAD)
SELECT year, season, city, total_athletes, 
       prev_edition_athletes, growth, growth_pct
FROM FDBO.WF_LAG_LEAD_EDITIONS_V 
WHERE season = 'Summer'
ORDER BY year;

-- Q18. Window: Quartile medaliști (NTILE, PERCENT_RANK, CUME_DIST)
SELECT athlete_name, country_name, total_medals, quartile, 
       ROUND(percent_rank, 4) AS pct_rank, ROUND(cume_dist, 4) AS cume
FROM FDBO.WF_ATHLETE_QUARTILES_V
WHERE quartile = 1
ORDER BY total_medals DESC
FETCH FIRST 15 ROWS ONLY;

-- Q19. Window: Procentul medaliilor per țară (RATIO_TO_REPORT)
SELECT country_name, total_medals, gold, 
       pct_of_all_medals, pct_of_all_gold, rank_total
FROM FDBO.WF_MEDAL_SHARE_V 
WHERE rank_total <= 20 OR country_name = 'Romania'
ORDER BY rank_total;

-- Q20. Window: Top sporturi per țară România (LISTAGG + ROW_NUMBER)
SELECT country_name, sport_name, sport_medals, sport_rank, all_sports_ranked
FROM FDBO.WF_COUNTRY_TOP_SPORTS_V
WHERE country_name = 'Romania'
ORDER BY sport_rank;
