defmodule Realtime.Tenants.TempStateStoreCapTest do
  use Realtime.DataCase, async: false

  alias Realtime.Tenants.Connect
  alias Realtime.Tenants.TempStateStore

  setup do
    prev = Application.get_env(:realtime, :temp_state_store_max_per_tenant)
    Application.put_env(:realtime, :temp_state_store_max_per_tenant, 2)

    on_exit(fn ->
      case prev do
        nil -> Application.delete_env(:realtime, :temp_state_store_max_per_tenant)
        value -> Application.put_env(:realtime, :temp_state_store_max_per_tenant, value)
      end
    end)

    tenant = Containers.checkout_tenant(run_migrations: true)
    {:ok, _db_conn} = Connect.lookup_or_start_connection(tenant.external_id)
    %{tenant: tenant}
  end

  test "rejects new stores once the limit is reached", %{tenant: tenant} do
    assert {:ok, _} = TempStateStore.start(tenant, self(), "room:" <> random_string())
    assert {:ok, _} = TempStateStore.start(tenant, self(), "room:" <> random_string())

    assert {:error, :too_many_state_stores} = TempStateStore.start(tenant, self(), "room:" <> random_string())
  end

  test "capacity_limit/1 is bounded by both the ceiling and the connection fraction" do
    # max_per_tenant is 2 in this suite; fraction default is 0.1
    assert TempStateStore.capacity_limit(1000) == 2
    assert TempStateStore.capacity_limit(20) == 2
    assert TempStateStore.capacity_limit(10) == 1
    # tiny databases still get one slot
    assert TempStateStore.capacity_limit(1) == 1
  end

  describe "input limits" do
    setup %{tenant: tenant} do
      Application.put_env(:realtime, :temp_state_store_max_value_bytes, 100)
      Application.put_env(:realtime, :temp_state_store_max_keys, 2)

      on_exit(fn ->
        Application.delete_env(:realtime, :temp_state_store_max_value_bytes)
        Application.delete_env(:realtime, :temp_state_store_max_keys)
      end)

      {:ok, store} = TempStateStore.start(tenant, self(), "room:" <> random_string())
      %{store: store}
    end

    test "rejects values over the configured byte limit", %{store: store} do
      assert {:error, :value_too_large} = TempStateStore.put(store, "k", %{"blob" => String.duplicate("x", 200)})
      assert {:ok, _} = TempStateStore.put(store, "k", %{"small" => 1})
    end

    test "rejects keys over the byte limit", %{store: store} do
      assert {:error, :key_too_large} = TempStateStore.put(store, String.duplicate("k", 2000), %{})
    end

    test "rejects new keys past max_keys but still allows updating existing keys", %{store: store} do
      assert {:ok, _} = TempStateStore.put(store, "a", %{})
      assert {:ok, _} = TempStateStore.put(store, "b", %{})
      assert {:error, :limit_reached} = TempStateStore.put(store, "c", %{})
      assert {:error, :limit_reached} = TempStateStore.insert(store, "c", %{})

      assert {:ok, 2} = TempStateStore.put(store, "a", %{"updated" => true})
    end

    test "deleting a key frees a slot for a new key", %{store: store} do
      assert {:ok, _} = TempStateStore.put(store, "a", %{})
      assert {:ok, _} = TempStateStore.put(store, "b", %{})
      assert {:error, :limit_reached} = TempStateStore.put(store, "c", %{})

      assert {:ok, :deleted} = TempStateStore.delete(store, "a")
      assert {:ok, 1} = TempStateStore.put(store, "c", %{})
    end
  end

  test "a store that stops frees a slot", %{tenant: tenant} do
    {:ok, store} = TempStateStore.start(tenant, self(), "room:" <> random_string())
    {:ok, _} = TempStateStore.start(tenant, self(), "room:" <> random_string())

    assert {:error, :too_many_state_stores} = TempStateStore.start(tenant, self(), "room:" <> random_string())

    ref = Process.monitor(store)
    GenServer.stop(store)
    assert_receive {:DOWN, ^ref, :process, ^store, _reason}, 2000

    # start/3 is async, so {:ok, pid} alone does not prove the database backstop passed;
    # a successful write does.
    assert eventually(fn ->
             case TempStateStore.start(tenant, self(), "room:" <> random_string()) do
               {:ok, pid} -> match?({:ok, _}, TempStateStore.put(pid, "k", %{}))
               _ -> false
             end
           end)
  end
end
