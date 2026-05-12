CREATE OR REPLACE VIEW vw_analiza_rank_atleti AS
SELECT 
    Athlete_Name, Sport, COUNT(*) as Medals,
    DENSE_RANK() OVER (PARTITION BY Sport ORDER BY COUNT(*) DESC) as Loc_In_Clasament
FROM vw_consolidare_olimpica WHERE Medal != 'NA'
GROUP BY Athlete_Name, Sport;

CREATE OR REPLACE VIEW vw_analiza_evolutie_participare AS
SELECT 
    Country_Name, Year, 
    COUNT(DISTINCT Athlete_ID) as Atleti_Prezenti,
    LAG(COUNT(DISTINCT Athlete_ID)) OVER (PARTITION BY Country_Name ORDER BY Year) as Atleti_Editia_Trecuta
FROM vw_consolidare_olimpica
GROUP BY Country_Name, Year;

CREATE OR REPLACE VIEW vw_analiza_comparativa_inaltime AS
SELECT 
    Athlete_Name, Sport, Height,
    AVG(Height) OVER (PARTITION BY Sport) as Medie_Inaltime_Sport
FROM vw_consolidare_olimpica WHERE Height IS NOT NULL;

SAVEPOINT sp_start_update;
UPDATE athletes SET Height = Height + 1 WHERE ID = 1;
ROLLBACK TO SAVEPOINT sp_start_update;

INSERT INTO athletes (ID, Name, Sex) VALUES (777777, 'Atlet Demonstrativ', 'M');
COMMIT;

SET TRANSACTION READ ONLY;
SELECT * FROM vw_analiza_rank_atleti;
COMMIT;