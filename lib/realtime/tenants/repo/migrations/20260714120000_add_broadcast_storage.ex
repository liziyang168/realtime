defmodule Realtime.Tenants.Migrations.AddBroadcastStorage do
  @moduledoc false
  use Ecto.Migration

  def up do
    execute("ALTER TABLE realtime.messages ADD COLUMN IF NOT EXISTS broadcasted_at timestamp")

    execute("""
    CREATE TABLE IF NOT EXISTS realtime.channels (
      id bigserial PRIMARY KEY,
      topic text NOT NULL,
      broadcast_storage_enabled_at timestamp,
      inserted_at timestamp NOT NULL DEFAULT now(),
      updated_at timestamp NOT NULL DEFAULT now()
    )
    """)

    execute("""
    DO $$ BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'channels_topic_index') THEN
        ALTER TABLE realtime.channels ADD CONSTRAINT channels_topic_index UNIQUE (topic);
      END IF;
    END $$;
    """)

    execute("ALTER TABLE realtime.channels OWNER TO supabase_realtime_admin")
    execute("GRANT SELECT, INSERT, UPDATE, DELETE ON realtime.channels TO postgres, anon, authenticated, service_role")
    execute("GRANT USAGE ON SEQUENCE realtime.channels_id_seq TO postgres, anon, authenticated, service_role")

    execute("""
    CREATE OR REPLACE FUNCTION realtime.enable_broadcast_storage(topic text)
    RETURNS boolean
    AS $$
    BEGIN
      INSERT INTO realtime.channels (topic, broadcast_storage_enabled_at)
      VALUES (topic, now())
      ON CONFLICT ON CONSTRAINT channels_topic_index DO UPDATE
        SET broadcast_storage_enabled_at = now(),
            updated_at = now();

      RETURN true;
    END;
    $$ LANGUAGE plpgsql;
    """)

    execute("""
    CREATE OR REPLACE FUNCTION realtime.disable_broadcast_storage(topic text)
    RETURNS boolean
    AS $$
    BEGIN
      UPDATE realtime.channels
      SET broadcast_storage_enabled_at = NULL, updated_at = now()
      WHERE channels.topic = disable_broadcast_storage.topic;

      RETURN true;
    END;
    $$ LANGUAGE plpgsql;
    """)

    execute("ALTER FUNCTION realtime.enable_broadcast_storage(text) OWNER TO supabase_realtime_admin")
    execute("ALTER FUNCTION realtime.disable_broadcast_storage(text) OWNER TO supabase_realtime_admin")
  end

  def down do
    execute("DROP FUNCTION IF EXISTS realtime.disable_broadcast_storage(text)")
    execute("DROP FUNCTION IF EXISTS realtime.enable_broadcast_storage(text)")
    execute("DROP TABLE IF EXISTS realtime.channels")
    execute("ALTER TABLE realtime.messages DROP COLUMN IF EXISTS broadcasted_at")
  end
end
