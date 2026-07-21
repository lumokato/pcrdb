FROM postgres:17-alpine

COPY src/pcrdb/db/schema.sql /docker-entrypoint-initdb.d/001-base-schema.sql
COPY src/pcrdb/db/migrations /opt/pcrdb/migrations
COPY docker/postgres/grant_roles.sql /opt/pcrdb/grant_roles.sql
COPY docker/postgres/entrypoint.sh /usr/local/bin/pcrdb-entrypoint

RUN chmod 0755 /usr/local/bin/pcrdb-entrypoint

ENTRYPOINT ["/usr/local/bin/pcrdb-entrypoint"]
