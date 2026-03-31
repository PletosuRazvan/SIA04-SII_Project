DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'olympics') THEN
    CREATE ROLE olympics WITH
      LOGIN
      NOSUPERUSER
      NOCREATEDB
      NOCREATEROLE
      INHERIT
      NOREPLICATION
      CONNECTION LIMIT -1
      PASSWORD 'olympics';
  END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS olympics AUTHORIZATION olympics;

SELECT n.nspname AS schema, r.rolname AS owner
FROM pg_namespace n
JOIN pg_roles r ON r.oid = n.nspowner
WHERE n.nspname = 'olympics';
