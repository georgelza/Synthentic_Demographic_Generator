
-- psql -h localhost -p 5432 -U dbadmin -d sales
-- psql -h localhost -p 5432 -U dbadmin -d sales
  
CREATE TABLE adults (
    id            SERIAL      NOT NULL,
    uniqueId      varchar(14) NOT NULL, 
    data          JSONB,
    created_at    timestamptz DEFAULT NOW() NOT NULL,
    PRIMARY KEY   (uniqueId)
) TABLESPACE pg_default;

CREATE TABLE children (
    id            SERIAL      NOT NULL,
    uniqueId      varchar(14) NOT NULL, 
    data          JSONB,
    created_at    timestamptz DEFAULT NOW() NOT NULL,
    PRIMARY KEY   (uniqueId)
) TABLESPACE pg_default;

CREATE TABLE families (
    id            SERIAL      NOT NULL,
    data          JSONB,
    created_at    timestamptz DEFAULT NOW() NOT NULL,
    PRIMARY KEY   (id)
) TABLESPACE pg_default;


CREATE INDEX adults_uniqueId_uidx ON public.adults (uniqueId);
CREATE INDEX children_uniqueId_uidx ON public.children (uniqueId);
CREATE INDEX families_id_uidx ON public.families (id);


ALTER TABLE public.adults REPLICA IDENTITY FULL;
ALTER TABLE public.adults OWNER TO dbadmin;

ALTER TABLE public.children REPLICA IDENTITY FULL;
ALTER TABLE public.children OWNER TO dbadmin;

ALTER TABLE public.families REPLICA IDENTITY FULL;
ALTER TABLE public.families OWNER TO dbadmin;


-- Replication role and user
CREATE ROLE replicator WITH
  LOGIN
  INHERIT
  SUPERUSER
  REPLICATION;

GRANT pg_monitor TO replicator;
GRANT ALL PRIVILEGES ON DATABASE adults    TO replicator;
GRANT ALL PRIVILEGES ON DATABASE children  TO replicator;
GRANT ALL PRIVILEGES ON DATABASE families  TO replicator;

CREATE ROLE flinkcdc WITH
  LOGIN
  INHERIT
  SUPERUSER
  REPLICATION
  ENCRYPTED PASSWORD 'dbpassword';

GRANT replicator TO flinkcdc;
GRANT replicator TO dbadmin;



-- replica is the default value for wal_level

-- https://dba.stackexchange.com/questions/270365/wal-level-set-to-replica-at-database-level-but-i-dont-see-that-in-configuration

-- select name, setting, sourcefile, sourceline from pg_settings where name = 'wal_level';

-- select pg_drop_replication_slot('the_name_of_subscriber');

-- https://www.dbi-services.com/blog/postgresql-when-wal_level-to-logical/
-- SELECT * FROM pg_logical_slot_get_changes('flinkcdc', NULL, NULL);

-- show config_file
-- show hba_file


-- See: 
-- https://www.alibabacloud.com/help/en/flink/developer-reference/configure-a-postgresql-database#concept-2116236
-- https://www.percona.com/blog/setting-up-streaming-replication-postgresql/#:~:text=PostgreSQL%20streaming%20replication%20is%20a,mirror%20the%20primary%20database%20accurately.
-- https://knowledge.informatica.com/s/article/ERROR-Must-be-wal-level-logical-for-logical-decoding-SQL-state-55000-while-performing-row-test-in-PowerExchange-PostgreSQL?language=en_US
-- https://www.dbi-services.com/blog/postgresql-when-wal_level-to-logical/