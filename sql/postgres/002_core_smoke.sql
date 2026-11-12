-- Read-only assertions; fixture mutations are rolled back in the shell test.
DO $$
BEGIN
    IF current_setting('wal_level') <> 'logical' THEN
        RAISE EXCEPTION 'wal_level must be logical';
    END IF;
    IF current_setting('max_slot_wal_keep_size') <> '5GB' THEN
        RAISE EXCEPTION 'WAL retention guard missing';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication_tables
        WHERE pubname = 'dbz_pub' AND schemaname = 'walmart' AND tablename = 'orders'
    ) THEN
        RAISE EXCEPTION 'CDC publication missing';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_class
        WHERE oid = 'walmart.orders'::regclass AND relreplident = 'f'
    ) THEN
        RAISE EXCEPTION 'Replica identity must be FULL';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'debezium_repl' AND rolreplication
    ) THEN
        RAISE EXCEPTION 'Replication privilege missing';
    END IF;
END
$$;
