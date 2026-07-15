defmodule Realtime.Tenants.Migrations.DefaultSchemaMigrationsInsertedAt do
  @moduledoc false

  use Ecto.Migration

  def up do
    execute("ALTER TABLE realtime.schema_migrations ALTER COLUMN inserted_at SET DEFAULT now()")
  end

  def down do
    execute("ALTER TABLE realtime.schema_migrations ALTER COLUMN inserted_at DROP DEFAULT")
  end
end
