BEGIN
  EXECUTE IMMEDIATE 'DROP DATABASE LINK PG_LINK';
EXCEPTION WHEN OTHERS THEN NULL;
END;

CREATE DATABASE LINK PG_LINK
   CONNECT TO "olympics" IDENTIFIED BY "olympics"
   USING '(DESCRIPTION =
     (ADDRESS = (PROTOCOL = TCP)(HOST = localhost)(PORT = 1521))
     (CONNECT_DATA =
       (SID = PG)
     )
     (HS = OK)
   )';

SELECT * FROM user_db_links;

SELECT db_link, username, host
FROM user_db_links
WHERE db_link = 'PG_LINK';

SELECT COUNT(*) FROM "athletes"@PG_LINK;
SELECT * FROM "athletes"@PG_LINK WHERE ROWNUM <= 5;
