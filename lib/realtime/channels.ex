defmodule Realtime.Channels do
  @moduledoc """
  Channels management.

  Looks up channels by `topic` unique key.
  """
  import Ecto.Query

  require Cachex.Spec

  alias Realtime.Api.Channel
  alias Realtime.Tenants.Repo

  def child_spec(_) do
    tenant_cache_expiration = Application.get_env(:realtime, :tenant_cache_expiration)

    %{
      id: __MODULE__,
      start: {Cachex, :start_link, [__MODULE__, [expiration: Cachex.Spec.expiration(default: tenant_cache_expiration)]]}
    }
  end

  @doc """
  Whether a topic currently has broadcast storage enabled for a tenant.

  Returns true when enabled, false otherwise.

  This is a best-effort operation, it will return `false` in case of errors.

  See `realtime.enable_broadcast_storage/2` to enable storage for a channel.

  Automatically uses RPC if the database connection is not on the same node.
  """
  @spec storage_enabled?(DBConnection.conn() | nil, String.t(), String.t()) :: boolean()
  def storage_enabled?(nil, _tenant_id, _topic), do: false

  def storage_enabled?(db_conn, tenant_id, topic) when node(db_conn) == node() do
    case fetch_storage_enabled(tenant_id, db_conn, topic) do
      {:ok, enabled?} -> enabled?
      {:error, _} -> false
    end
  end

  def storage_enabled?(db_conn, tenant_id, topic) do
    Realtime.GenRpc.call(node(db_conn), __MODULE__, :storage_enabled?, [db_conn, tenant_id, topic], key: topic) == true
  end

  defp fetch_storage_enabled(tenant_id, db_conn, topic) do
    query = from(c in Channel, where: c.topic == ^topic)

    case Cachex.fetch(__MODULE__, cache_key(tenant_id, topic), fn _key ->
           case Repo.one(db_conn, query, Channel) do
             {:ok, %Channel{broadcast_storage_enabled_at: enabled_at}} -> {:commit, not is_nil(enabled_at)}
             {:error, :not_found} -> {:commit, false}
             {:error, _} = error -> {:ignore, error}
           end
         end) do
      {:commit, enabled?} -> {:ok, enabled?}
      {:ok, enabled?} -> {:ok, enabled?}
      {:ignore, error} -> error
    end
  end

  defp cache_key(tenant_id, topic), do: {tenant_id, topic}
end
