defmodule Realtime.ChannelsTest do
  use Realtime.DataCase, async: false

  alias Realtime.Channels
  alias Realtime.Tenants.Connect

  setup do
    tenant = Containers.checkout_tenant(run_migrations: true)
    {:ok, db_conn} = Connect.lookup_or_start_connection(tenant.external_id)
    assert Connect.ready?(tenant.external_id)

    %{tenant: tenant, db_conn: db_conn}
  end

  describe "storage_enabled?/3" do
    test "returns false without a database connection", %{tenant: tenant} do
      refute Channels.storage_enabled?(nil, tenant.external_id, random_string())
    end

    test "returns false for a topic that never enabled storage", %{tenant: tenant, db_conn: db_conn} do
      refute Channels.storage_enabled?(db_conn, tenant.external_id, random_string())
    end

    test "returns true once a topic enables storage", %{tenant: tenant, db_conn: db_conn} do
      topic = random_string()
      refute Channels.storage_enabled?(db_conn, tenant.external_id, topic)
      enable_broadcast_storage(db_conn, tenant, topic)
      assert Channels.storage_enabled?(db_conn, tenant.external_id, topic)
    end

    test "returns false again once storage is disabled", %{tenant: tenant, db_conn: db_conn} do
      topic = random_string()

      enable_broadcast_storage(db_conn, tenant, topic)
      assert Channels.storage_enabled?(db_conn, tenant.external_id, topic)

      disable_broadcast_storage(db_conn, tenant, topic)
      refute Channels.storage_enabled?(db_conn, tenant.external_id, topic)
    end

    test "enabling storage again for the same channel updates the existing row instead of duplicating it", %{
      db_conn: db_conn
    } do
      topic = random_string()

      Postgrex.query!(db_conn, "SELECT realtime.enable_broadcast_storage($1)", [topic])
      Postgrex.query!(db_conn, "SELECT realtime.enable_broadcast_storage($1)", [topic])

      assert {:ok, %Postgrex.Result{rows: [[1]]}} =
               Postgrex.query(db_conn, "SELECT count(*) FROM realtime.channels WHERE topic = $1", [topic])
    end

    test "distributed storage_enabled?", %{tenant: tenant, db_conn: db_conn} do
      topic = random_string()
      enable_broadcast_storage(db_conn, tenant, topic)

      {:ok, node} = Clustered.start()

      # Call remote node passing the database connection that is local to this node
      assert :erpc.call(node, Channels, :storage_enabled?, [db_conn, tenant.external_id, topic])
    end
  end

  describe "disable_broadcast_storage/1" do
    test "is a no-op that still returns success when the channel was never enabled", %{db_conn: db_conn} do
      assert {:ok, %Postgrex.Result{rows: [[true]]}} =
               Postgrex.query(db_conn, "SELECT realtime.disable_broadcast_storage($1)", [random_string()])
    end

    test "nullify the flag rather than deleting the row", %{tenant: tenant, db_conn: db_conn} do
      topic = random_string()

      enable_broadcast_storage(db_conn, tenant, topic)
      assert Channels.storage_enabled?(db_conn, tenant.external_id, topic)

      disable_broadcast_storage(db_conn, tenant, topic)
      refute Channels.storage_enabled?(db_conn, tenant.external_id, topic)

      assert {:ok, %Postgrex.Result{rows: [[nil]]}} =
               Postgrex.query(
                 db_conn,
                 "SELECT broadcast_storage_enabled_at FROM realtime.channels WHERE topic = $1",
                 [topic]
               )
    end
  end
end
