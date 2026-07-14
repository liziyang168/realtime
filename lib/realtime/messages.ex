defmodule Realtime.Messages do
  @moduledoc """
  Handles `realtime.messages` table operations
  """

  alias Realtime.Api.Message
  alias Realtime.Channels
  alias Realtime.Tenants.Connect
  alias Realtime.Tenants.Repo

  import Ecto.Query, only: [from: 2]

  @hard_limit 25
  @default_timeout 5_000

  @doc """
  Persists a broadcast sent over WebSocket for `topic` if storage is enabled for such topic.

  Bypass RLS because the check has already been done on the message broadcast,
  and then set `broadcasted_at` to avoid re-broadcasting the message twice.

  Automatically uses RPC if the database connection is not on the same node.
  """
  @spec store(DBConnection.conn(), String.t(), String.t(), String.t(), term(), boolean()) ::
          {:ok, binary()} | {:error, any()} | {:error, :rpc_error, term} | {:error, :storage_disabled}
  def store(conn, tenant_id, topic, event, payload, private) when node(conn) == node() do
    if Channels.storage_enabled?(conn, tenant_id, topic) do
      insert(conn, topic, event, payload, private)
    else
      {:error, :storage_disabled}
    end
  end

  def store(conn, tenant_id, topic, event, payload, private) do
    Realtime.GenRpc.call(node(conn), __MODULE__, :store, [conn, tenant_id, topic, event, payload, private], key: topic)
  end

  @doc """
  Similar to `store/6` but runs asynchronously for callers that don't already hold a connection.
  """
  @spec store_async(String.t(), String.t(), String.t(), term()) :: :ok
  def store_async(tenant_id, topic, event, payload) do
    Task.Supervisor.start_child(Realtime.TaskSupervisor, fn ->
      case Connect.lookup_or_start_connection(tenant_id) do
        {:ok, conn} -> store(conn, tenant_id, topic, event, payload, false)
        {:error, _reason} -> :skip
      end
    end)

    :ok
  end

  defp insert(conn, topic, event, payload, private) do
    changeset =
      Message.changeset(%Message{}, %{
        topic: topic,
        extension: :broadcast,
        event: event,
        payload: payload,
        private: private,
        broadcasted_at: NaiveDateTime.utc_now(:microsecond)
      })

    case Repo.insert(conn, changeset, Message) do
      {:ok, %Message{id: id}} -> {:ok, id}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Fetch last `limit ` messages for a given `topic` inserted after `since`

  Automatically uses RPC if the database connection is not in the same node

  Only allowed for private channels
  """
  @spec replay(pid, String.t(), String.t(), non_neg_integer, non_neg_integer) ::
          {:ok, Message.t(), [String.t()]} | {:error, term} | {:error, :rpc_error, term}
  def replay(conn, tenant_id, topic, since, limit)
      when node(conn) == node() and is_integer(since) and is_integer(limit) do
    limit = max(min(limit, @hard_limit), 1)

    with {:ok, since} <- DateTime.from_unix(since, :millisecond),
         {:ok, messages} <- messages(conn, tenant_id, topic, since, limit) do
      {:ok, Enum.reverse(messages), MapSet.new(messages, & &1.id)}
    else
      {:error, :postgrex_exception} -> {:error, :failed_to_replay_messages}
      {:error, :invalid_unix_time} -> {:error, :invalid_replay_params}
      error -> error
    end
  end

  def replay(conn, tenant_id, topic, since, limit) when is_integer(since) and is_integer(limit) do
    Realtime.GenRpc.call(node(conn), __MODULE__, :replay, [conn, tenant_id, topic, since, limit],
      key: topic,
      tenant_id: tenant_id
    )
  end

  def replay(_, _, _, _, _), do: {:error, :invalid_replay_params}

  defp messages(conn, tenant_id, topic, since, limit) do
    since = DateTime.to_naive(since)
    # We want to avoid searching partitions in the future as they should be empty
    # so we limit to 1 minute in the future to account for any potential drift
    now = NaiveDateTime.utc_now() |> NaiveDateTime.add(1, :minute)

    query =
      from m in Message,
        where:
          m.topic == ^topic and
            m.private == true and
            m.extension == :broadcast and
            m.inserted_at >= ^since and
            m.inserted_at < ^now,
        limit: ^limit,
        order_by: [desc: m.inserted_at]

    {latency, value} =
      :timer.tc(Realtime.Tenants.Repo, :all, [conn, query, Message, [timeout: @default_timeout]], :millisecond)

    :telemetry.execute([:realtime, :tenants, :replay], %{latency: latency}, %{tenant: tenant_id})
    value
  end

  @doc """
  Deletes messages older than 72 hours for a given tenant connection
  """
  @spec delete_old_messages(pid()) :: :ok
  def delete_old_messages(conn) do
    limit =
      NaiveDateTime.utc_now()
      |> NaiveDateTime.add(-72, :hour)
      |> NaiveDateTime.to_date()

    %{rows: rows} =
      Postgrex.query!(
        conn,
        """
        SELECT child.relname
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child ON pg_inherits.inhrelid = child.oid
        JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace
        JOIN pg_namespace nmsp_child ON nmsp_child.oid = child.relnamespace
        WHERE parent.relname = 'messages'
        AND nmsp_child.nspname = 'realtime'
        """,
        []
      )

    rows
    |> Enum.filter(fn ["messages_" <> date] ->
      date |> String.replace("_", "-") |> Date.from_iso8601!() |> Date.compare(limit) == :lt
    end)
    |> Enum.each(&Postgrex.query!(conn, "DROP TABLE IF EXISTS realtime.#{&1}", []))

    :ok
  end
end
