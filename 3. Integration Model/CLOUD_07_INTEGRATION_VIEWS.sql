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
LEFT JOIN OLY_REF.COUNTRIES c ON r.noc         = c.noc;

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
LEFT JOIN OLY_REF.COUNTRIES c ON a.noc = c.noc;

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
FROM OLY_REF.GAMES g;

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
JOIN OLY_REF.SPORTS s ON e.sport_id = s.sport_id;

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
FROM OLY_REF.COUNTRIES c;

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
LEFT JOIN OLY_REF.GAMES g ON r.game_id = g.game_id;

SELECT view_name FROM all_views WHERE owner = 'FDBO' ORDER BY view_name;

SELECT COUNT(*) AS total_rows FROM FDBO.INT_RESULTS_FULL_V;

SELECT * FROM FDBO.INT_RESULTS_FULL_V WHERE ROWNUM <= 5;
