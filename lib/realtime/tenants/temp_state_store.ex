defmodule Realtime.Tenants.TempStateStore do
  @moduledoc """
  Session-local, channel-scoped key/value state backed by a PostgreSQL temporary table.

  This is an opt-in feature: a private channel asks for it through the join payload
  (`config.state.enabled = true`) and a dedicated process is started for that channel. It is only
  honoured on private (authenticated) channels because it opens a dedicated session against the
  tenant database; allowing it on public channels would let anyone spin up sessions and write to
  the tenant database. Note that beyond the private-channel gate there is no per-feature
  authorization: any user allowed to join the private channel gets full access to their own
  store (state is per-connection scratch space, never shared across users).

  ## Ownership model

      one process owns one connection
      one connection owns one temp table
      all mutations go through that owner

  Each opted-in channel gets its own `Realtime.Tenants.TempStateStore` process that owns a
  dedicated Postgrex connection (pool size 1) against the tenant database. The scope is the
  *channel process* (one socket join), not the topic: two clients on the same private topic get
  two independent stores, connections and temp tables. This is per-connection scratch space, not
  shared state broadcast to a topic's subscribers.

  `start/3` returns as soon as the store process is registered; the database connection, session
  setup and `TEMP TABLE` creation happen asynchronously in a `handle_continue` so channel joins
  never block on the tenant database and a slow tenant cannot stall the shared supervisor.
  Commands sent before setup completes simply queue behind it. If the connection cannot be
  established (or the capacity check below fails) the store stops; the channel monitors the
  store and notifies the client that state is unavailable.

  The connection uses `backoff_type: :stop`, so a dropped connection is not retried: the store
  stops instead and the channel re-creates one on its next join if it still wants state. The
  store also monitors the tenant's `Realtime.Tenants.Connect` process and stops when it goes
  down, so tenant-level teardown (rebalancing, database settings changes) also closes these
  sessions instead of leaving them behind. The temp table therefore tracks the lifetime of a
  single session.

  ## Per-tenant limit

  Because each store holds a dedicated tenant-database session, the number of live stores is
  capped in two layers, both bounded by `capacity_limit/1` (`min(max_per_tenant(),
  max_connection_fraction() * max_connections)`):

    * a node-local gate: store starts are serialized through the DynamicSupervisor and counted
      in `Realtime.Registry`, so on a single node the cap is exact and `start/3` returns
      `{:error, :too_many_state_stores}` synchronously;
    * a cluster-wide backstop: after connecting, the store counts the live
      `realtime_temp_state` sessions in the tenant database (`pg_stat_activity`, which includes
      its own session) and stops itself if the limit is exceeded.

  The backstop is check-after-connect rather than check-then-act, so a cross-node burst can
  transiently open more sessions than the cap, but the surplus stores terminate themselves and
  the steady state converges to the limit. This capacity is also reserved in
  `Realtime.Database.check_tenant_connection/1` so tenant provisioning accounts for it.

  Because the table is a `TEMP TABLE` it lives in `pg_temp` and is therefore:

    * connection-local
    * disposable and rebuildable
    * gone when the session ends

  The supported SQL surface is intentionally tiny and only ever touches the channel's own
  temp table by primary key: `put`, `insert`, `update`, `delete`, `get`, `clear` and `count`.
  No joins, no sorts, no aggregation beyond the `count(*)` health check, and no access to
  any other table. Values are bound as JSON text and cast server-side (`::text::jsonb`), so
  they are stored as real jsonb documents and the size limit measures exactly what is stored.

  ## Input limits

  To keep unbounded data out of the tenant's temp space, writes are bounded in the application:

    * keys must be strings of at most 1024 bytes (`:invalid_key` / `:key_too_large`)
    * values larger than `max_value_bytes/0` (JSON-encoded) are rejected (`:value_too_large`)
    * a store holds at most `max_keys/0` keys; a new key past that returns `:limit_reached`
      (updates to existing keys are still allowed). The key count is tracked in the owner
      process — exact, because all mutations are serialized through it — so writes pay no
      extra SQL for limit enforcement.

  These are enforced regardless of the session guardrails below, which cannot be fully relied on.

  > #### Not guaranteed RAM-only {: .warning}
  >
  > PostgreSQL temp tables are not strictly guaranteed to be memory-only. We raise `temp_buffers`
  > and keep the workload to primary-key access. We also *attempt* a low `temp_file_limit`, but
  > that parameter is superuser-only (`SUSET`) and is silently skipped for the least-privilege
  > tenant role used at runtime — so it is not a reliable guard. The application-level input limits
  > above are what actually bound storage; core PostgreSQL provides no hard "RAM-only" mode.
  """
  use GenServer, restart: :temporary
  use Realtime.Logs

  alias Realtime.Api.Tenant
  alias Realtime.Database
  alias Realtime.Tenants.Connect

  @application_name "realtime_temp_state"
  @default_max_per_tenant 10
  @default_max_connection_fraction 0.1
  @default_max_value_bytes 256_000
  @default_max_keys 10_000
  @max_key_bytes 1024

  # Query timeout for every statement; the GenServer.call timeout must exceed it so a slow
  # query is reported as its real outcome instead of a caller timeout for a write that
  # actually committed.
  @query_timeout 15_000
  @call_timeout @query_timeout + 5_000
  @idle_interval 30_000

  # Runs on the store's own session, so the count includes this session.
  @capacity_query """
  SELECT count(*), current_setting('max_connections')::int
  FROM pg_stat_activity
  WHERE application_name = $1 AND datname = current_database()
  """

  @session_settings [
    "SET temp_buffers = '32MB'",
    "SET work_mem = '4MB'",
    "SET temp_file_limit = '1MB'"
  ]

  @type version :: non_neg_integer()
  @type expected :: version() | nil

  defstruct [:tenant, :conn, :table, :monitored_pid, :channel_ref, :connect_ref, key_count: 0]

  ## Public API

  @doc """
  Starts a temp state store for a channel under the dedicated DynamicSupervisor.

  `monitored_pid` is the channel process: when it goes down the store stops and the session
  (and therefore the temp table) is torn down. Returns `{:error, :too_many_state_stores}` when
  the node-local cap is already reached; the database connection itself is established
  asynchronously after this returns.
  """
  @spec start(Tenant.t(), pid(), String.t()) :: {:ok, pid()} | {:error, term()}
  def start(%Tenant{} = tenant, monitored_pid, channel_name) do
    opts = [tenant: tenant, monitored_pid: monitored_pid, channel_name: channel_name]
    DynamicSupervisor.start_child(__MODULE__.DynamicSupervisor, {__MODULE__, opts})
  end

  @doc "Effective cap on live stores for a database with the given `max_connections`."
  @spec capacity_limit(pos_integer()) :: pos_integer()
  def capacity_limit(max_connections) do
    from_database = max(1, floor(max_connections * max_connection_fraction()))
    min(max_per_tenant(), from_database)
  end

  @doc "Absolute ceiling on live stores per tenant database, regardless of `max_connections`."
  @spec max_per_tenant() :: pos_integer()
  def max_per_tenant do
    Application.get_env(:realtime, :temp_state_store_max_per_tenant, @default_max_per_tenant)
  end

  @doc "Fraction of the tenant database's `max_connections` that temp state stores may use."
  @spec max_connection_fraction() :: float()
  def max_connection_fraction do
    Application.get_env(:realtime, :temp_state_store_max_connection_fraction, @default_max_connection_fraction)
  end

  @doc "Maximum byte size of a single (JSON-encoded) value."
  @spec max_value_bytes() :: pos_integer()
  def max_value_bytes do
    Application.get_env(:realtime, :temp_state_store_max_value_bytes, @default_max_value_bytes)
  end

  @doc "Maximum number of keys a single store may hold."
  @spec max_keys() :: pos_integer()
  def max_keys do
    Application.get_env(:realtime, :temp_state_store_max_keys, @default_max_keys)
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc "Upsert a key. Returns `{:ok, version}`."
  @spec put(pid(), String.t(), term()) :: {:ok, integer()} | {:error, term()}
  def put(pid, key, value), do: call(pid, {:put, key, value})

  @doc "Insert a key that must not exist yet. Returns `{:ok, version}` or `{:error, :already_exists}`."
  @spec insert(pid(), String.t(), term()) :: {:ok, integer()} | {:error, term()}
  def insert(pid, key, value), do: call(pid, {:insert, key, value})

  @doc """
  Update a key that must already exist. Returns `{:ok, version}` or `{:error, :not_found}`.

  Pass `expected` (the version last read) for an optimistic, compare-and-set update: it only
  applies when the current version matches, otherwise returns `{:error, {:version_mismatch, current}}`
  so the caller can re-read and retry. With `nil` (the default) it is a last-write-wins update.
  """
  @spec update(pid(), String.t(), term(), expected()) :: {:ok, version()} | {:error, term()}
  def update(pid, key, value, expected \\ nil), do: call(pid, {:update, key, value, expected})

  @doc """
  Delete a key. Returns `{:ok, :deleted}` or `{:error, :not_found}`.

  Pass `expected` for a compare-and-set delete: it only applies when the current version matches,
  otherwise returns `{:error, {:version_mismatch, current}}`.
  """
  @spec delete(pid(), String.t(), expected()) :: {:ok, :deleted} | {:error, term()}
  def delete(pid, key, expected \\ nil), do: call(pid, {:delete, key, expected})

  @doc "Read a key by primary key. Returns `{:ok, %{value, version, updated_at}}` or `{:error, :not_found}`."
  @spec get(pid(), String.t()) :: {:ok, map()} | {:error, term()}
  def get(pid, key), do: call(pid, {:get, key})

  @doc "Remove all rows via `TRUNCATE`."
  @spec clear(pid()) :: :ok | {:error, term()}
  def clear(pid), do: call(pid, :clear)

  @doc "Health-check count of rows in the temp table. Not for the hot path."
  @spec count(pid()) :: {:ok, non_neg_integer()} | {:error, term()}
  def count(pid), do: call(pid, :count)

  defp call(pid, command) do
    GenServer.call(pid, {:command, command}, @call_timeout)
  catch
    :exit, _ -> {:error, :unavailable}
  end

  ## GenServer callbacks

  # init stays IO-free so store starts never block the shared DynamicSupervisor: it only
  # applies the node-local cap (exact, because starts are serialized through the supervisor
  # and registrations are cleaned up on process death) and registers monitors. The database
  # work happens in handle_continue.
  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    tenant = Keyword.fetch!(opts, :tenant)
    monitored_pid = Keyword.fetch!(opts, :monitored_pid)
    channel_name = Keyword.fetch!(opts, :channel_name)

    Logger.metadata(external_id: tenant.external_id, project: tenant.external_id)

    registry_key = {__MODULE__, tenant.external_id}

    if Registry.count_match(Realtime.Registry, registry_key, :_) >= max_per_tenant() do
      {:stop, :too_many_state_stores}
    else
      {:ok, _} = Registry.register(Realtime.Registry, registry_key, nil)
      channel_ref = Process.monitor(monitored_pid)

      state = %__MODULE__{
        tenant: tenant,
        table: table_name(channel_name),
        monitored_pid: monitored_pid,
        channel_ref: channel_ref
      }

      {:ok, state, {:continue, :connect}}
    end
  end

  @impl true
  def handle_continue(:connect, %{tenant: tenant, table: table} = state) do
    case connect(tenant, table) do
      {:ok, conn} ->
        # conn goes into state before the capacity check so terminate/2 closes the session
        # on every failure path from here on.
        state = %{state | conn: conn, tenant: nil}

        case within_capacity(conn) do
          :ok ->
            # Tie the session to the tenant's Connect lifecycle so tenant-level teardown
            # (rebalancing, db settings changes) also closes it. Lenient when Connect is not
            # registered (e.g. unit tests exercising the store directly).
            connect_ref =
              case Connect.whereis(tenant.external_id) do
                pid when is_pid(pid) -> Process.monitor(pid)
                _ -> nil
              end

            {:noreply, %{state | connect_ref: connect_ref}}

          {:error, :too_many_state_stores} ->
            log_warning("TempStateStoreLimitReached", "Per-tenant temp state store limit reached")
            {:stop, :normal, state}

          {:error, error} ->
            log_error("TempStateStoreConnectionError", error)
            {:stop, :normal, state}
        end

      {:error, error} ->
        log_error("TempStateStoreConnectionError", error)
        {:stop, :normal, state}
    end
  end

  @impl true
  def handle_call({:command, command}, _from, state) do
    {reply, state} = run(command, state)
    {:reply, reply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{channel_ref: ref} = state) do
    {:stop, :normal, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{connect_ref: ref} = state) do
    Logger.info("Tenant connection process terminated, stopping temp state store")
    {:stop, :normal, state}
  end

  def handle_info({:EXIT, conn, reason}, %{conn: conn} = state) do
    log_warning("TempStateStoreConnectionDown", reason)
    {:stop, :normal, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{conn: conn}) when is_pid(conn) do
    Process.exit(conn, :shutdown)
    :ok
  end

  def terminate(_reason, _state), do: :ok

  ## Commands
  #
  # Every clause returns {reply, state}. key_count is exact: this process serializes all
  # mutations, so counting inserted/deleted keys app-side replaces a count(*) scan per write.

  defp run({:put, key, value}, %{conn: conn, table: table, key_count: key_count} = state) do
    with {:ok, value} <- encode_and_validate(key, value) do
      if key_count < max_keys() do
        sql = """
        INSERT INTO #{table} (key, value)
        VALUES ($1, $2::text::jsonb)
        ON CONFLICT (key)
        DO UPDATE SET value = EXCLUDED.value,
                      version = #{table}.version + 1,
                      updated_at = clock_timestamp()
        RETURNING version
        """

        case query(conn, sql, [key, value]) do
          # version 1 can only come from a fresh insert; conflicting rows always bump past it
          {:ok, %{rows: [[1]]}} -> {{:ok, 1}, %{state | key_count: key_count + 1}}
          {:ok, %{rows: [[version]]}} -> {{:ok, version}, state}
          {:error, _} = error -> {error, state}
        end
      else
        # At the key cap only updates to existing keys are allowed
        case update_row(conn, table, key, value, nil) do
          {:ok, %{rows: [[version]]}} -> {{:ok, version}, state}
          {:ok, %{num_rows: 0}} -> {{:error, :limit_reached}, state}
          {:error, _} = error -> {error, state}
        end
      end
    else
      error -> {error, state}
    end
  end

  defp run({:insert, key, value}, %{conn: conn, table: table, key_count: key_count} = state) do
    with {:ok, value} <- encode_and_validate(key, value) do
      if key_count < max_keys() do
        sql = "INSERT INTO #{table} (key, value) VALUES ($1, $2::text::jsonb) RETURNING version"

        case query(conn, sql, [key, value]) do
          {:ok, %{rows: [[version]]}} -> {{:ok, version}, %{state | key_count: key_count + 1}}
          {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} -> {{:error, :already_exists}, state}
          {:error, _} = error -> {error, state}
        end
      else
        {{:error, :limit_reached}, state}
      end
    else
      error -> {error, state}
    end
  end

  defp run({:update, key, value, expected}, %{conn: conn, table: table} = state) do
    with {:ok, value} <- encode_and_validate(key, value) do
      case update_row(conn, table, key, value, expected) do
        {:ok, %{rows: [[version]]}} -> {{:ok, version}, state}
        {:ok, %{num_rows: 0}} when is_nil(expected) -> {{:error, :not_found}, state}
        {:ok, %{num_rows: 0}} -> {version_conflict(conn, table, key), state}
        {:error, _} = error -> {error, state}
      end
    else
      error -> {error, state}
    end
  end

  defp run({:delete, key, expected}, %{conn: conn, table: table, key_count: key_count} = state) do
    with :ok <- validate_key(key) do
      {sql, params} =
        case expected do
          nil -> {"DELETE FROM #{table} WHERE key = $1", [key]}
          version -> {"DELETE FROM #{table} WHERE key = $1 AND version = $2", [key, version]}
        end

      case query(conn, sql, params) do
        {:ok, %{num_rows: 0}} when is_nil(expected) -> {{:error, :not_found}, state}
        {:ok, %{num_rows: 0}} -> {version_conflict(conn, table, key), state}
        {:ok, _} -> {{:ok, :deleted}, %{state | key_count: max(key_count - 1, 0)}}
        {:error, _} = error -> {error, state}
      end
    else
      error -> {error, state}
    end
  end

  defp run({:get, key}, %{conn: conn, table: table} = state) do
    with :ok <- validate_key(key) do
      sql = "SELECT value, version, updated_at FROM #{table} WHERE key = $1"

      case query(conn, sql, [key]) do
        {:ok, %{rows: [[value, version, updated_at]]}} ->
          {{:ok, %{value: value, version: version, updated_at: updated_at}}, state}

        {:ok, %{num_rows: 0}} ->
          {{:error, :not_found}, state}

        {:error, _} = error ->
          {error, state}
      end
    else
      error -> {error, state}
    end
  end

  defp run(:clear, %{conn: conn, table: table} = state) do
    case query(conn, "TRUNCATE #{table}", []) do
      {:ok, _} -> {:ok, %{state | key_count: 0}}
      {:error, _} = error -> {error, state}
    end
  end

  defp run(:count, %{conn: conn, table: table} = state) do
    case query(conn, "SELECT count(*) FROM #{table}", []) do
      {:ok, %{rows: [[count]]}} -> {{:ok, count}, state}
      {:error, _} = error -> {error, state}
    end
  end

  ## Private

  defp update_row(conn, table, key, value, expected) do
    {sql_tail, params_tail} =
      case expected do
        nil -> {"", []}
        version -> {" AND version = $3", [version]}
      end

    sql = """
    UPDATE #{table}
    SET value = $2::text::jsonb, version = version + 1, updated_at = clock_timestamp()
    WHERE key = $1#{sql_tail}
    RETURNING version
    """

    query(conn, sql, [key, value | params_tail])
  end

  defp query(conn, sql, params) do
    Postgrex.query(conn, sql, params, timeout: @query_timeout)
  end

  defp encode_and_validate(key, value) do
    with :ok <- validate_key(key),
         {:ok, encoded} <- encode(value) do
      if byte_size(encoded) > max_value_bytes(), do: {:error, :value_too_large}, else: {:ok, encoded}
    end
  end

  defp validate_key(key) when is_binary(key) and byte_size(key) <= @max_key_bytes, do: :ok
  defp validate_key(key) when is_binary(key), do: {:error, :key_too_large}
  defp validate_key(_key), do: {:error, :invalid_key}

  defp encode(value) do
    case Jason.encode(value) do
      {:ok, encoded} -> {:ok, encoded}
      {:error, _} -> {:error, :invalid_value}
    end
  rescue
    # Jason raises Protocol.UndefinedError for unencodable structs
    _ -> {:error, :invalid_value}
  end

  defp version_conflict(conn, table, key) do
    case query(conn, "SELECT version FROM #{table} WHERE key = $1", [key]) do
      {:ok, %{rows: [[version]]}} -> {:error, {:version_mismatch, version}}
      {:ok, %{num_rows: 0}} -> {:error, :not_found}
      {:error, _} = error -> error
    end
  end

  # The capacity query doubles as the readiness check: it blocks until the connection is
  # established (or fails), and its result enforces the cluster-wide cap from the session's
  # own point of view — the count includes this session, so strictly greater means over cap.
  defp within_capacity(conn) do
    case Postgrex.query(conn, @capacity_query, [@application_name], timeout: @query_timeout) do
      {:ok, %{rows: [[used, max_connections]]}} ->
        if used > capacity_limit(max_connections), do: {:error, :too_many_state_stores}, else: :ok

      {:error, _} = error ->
        error
    end
  end

  defp connect(tenant, table) do
    with {:ok, settings} <- Database.from_tenant(tenant, @application_name, :stop) do
      Postgrex.start_link(
        hostname: settings.hostname,
        port: settings.port,
        database: settings.database,
        username: settings.username,
        password: settings.password,
        pool_size: 1,
        queue_target: settings.queue_target,
        idle_interval: @idle_interval,
        parameters: [application_name: @application_name],
        socket_options: settings.socket_options,
        ssl: settings.ssl,
        backoff_type: :stop,
        after_connect: {__MODULE__, :setup_session, [table]}
      )
    end
  end

  @doc false
  def setup_session(conn, table) do
    Enum.each(@session_settings, fn setting ->
      case Postgrex.query(conn, setting, []) do
        {:ok, _} -> :ok
        {:error, error} -> log_warning("TempStateStoreSettingSkipped", %{setting: setting, error: error})
      end
    end)

    Postgrex.query!(
      conn,
      """
      CREATE TEMP TABLE IF NOT EXISTS #{table} (
        key text PRIMARY KEY,
        value jsonb NOT NULL,
        version bigint NOT NULL DEFAULT 1,
        updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
      ) ON COMMIT PRESERVE ROWS
      """,
      []
    )

    :ok
  end

  @doc false
  def table_name(channel_name) do
    sanitized =
      channel_name
      |> String.downcase()
      |> String.replace(~r/[^a-z0-9]+/, "_")
      |> String.slice(0, 30)
      |> String.trim("_")

    hash = :crypto.hash(:sha256, channel_name) |> Base.encode16(case: :lower) |> String.slice(0, 8)

    "realtime_state_#{sanitized}_#{hash}"
  end
end
