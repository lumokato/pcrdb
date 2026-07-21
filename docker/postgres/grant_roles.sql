\set ON_ERROR_STOP on
\getenv web_password PCRDB_WEB_PASSWORD
\getenv worker_password PCRDB_WORKER_PASSWORD

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pcrdb_web') THEN
        CREATE ROLE pcrdb_web LOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pcrdb_worker') THEN
        CREATE ROLE pcrdb_worker LOGIN;
    END IF;
END
$$;

SELECT format('ALTER ROLE pcrdb_web WITH LOGIN PASSWORD %L', :'web_password') \gexec
SELECT format('ALTER ROLE pcrdb_worker WITH LOGIN PASSWORD %L', :'worker_password') \gexec

GRANT CONNECT ON DATABASE :DBNAME TO pcrdb_web, pcrdb_worker;
GRANT USAGE ON SCHEMA public, auth, clan_battle TO pcrdb_web, pcrdb_worker;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public, auth TO pcrdb_web;
GRANT SELECT ON ALL TABLES IN SCHEMA clan_battle TO pcrdb_web;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public, auth TO pcrdb_web;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public, clan_battle TO pcrdb_worker;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public, clan_battle TO pcrdb_worker;

ALTER DEFAULT PRIVILEGES IN SCHEMA public, auth
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO pcrdb_web;
ALTER DEFAULT PRIVILEGES IN SCHEMA clan_battle
    GRANT SELECT ON TABLES TO pcrdb_web;
ALTER DEFAULT PRIVILEGES IN SCHEMA public, auth
    GRANT USAGE, SELECT ON SEQUENCES TO pcrdb_web;

ALTER DEFAULT PRIVILEGES IN SCHEMA public, clan_battle
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO pcrdb_worker;
ALTER DEFAULT PRIVILEGES IN SCHEMA public, clan_battle
    GRANT USAGE, SELECT ON SEQUENCES TO pcrdb_worker;
