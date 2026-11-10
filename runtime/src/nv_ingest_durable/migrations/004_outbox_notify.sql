BEGIN;

CREATE OR REPLACE FUNCTION durable_outbox_notify()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify($channel$durable_outbox$channel$, NEW.event_id::text);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS durable_outbox_insert_notify ON outbox_events;
CREATE TRIGGER durable_outbox_insert_notify
AFTER INSERT ON outbox_events
FOR EACH ROW EXECUTE FUNCTION durable_outbox_notify();

COMMIT;
