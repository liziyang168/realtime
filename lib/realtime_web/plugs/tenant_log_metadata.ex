defmodule RealtimeWeb.Plugs.TenantLogMetadata do
  @moduledoc """
  Tags the current process' Logger metadata with `external_id`/`project` as early as
  possible in the plug pipeline, before the endpoint's own request logging runs.

  This is a best-effort guess based on the request path/host alone - it does not look
  up the tenant. `RealtimeWeb.Plugs.AssignTenant` and `RealtimeWeb.AuthTenant` remain the
  source of truth and reassert this metadata once the tenant has actually been resolved.

  Only routes that actually resolve a tenant (mirroring the `tenant_api`/`secure_tenant_api`/
  `broadcast_single` router pipelines) are matched here - anything else is left untagged, so
  tenant-agnostic endpoints (e.g. `/api/tenants`, `/api/openapi`) never get a bogus tag.
  """
  require Logger

  alias Realtime.Database

  def init(opts), do: opts

  def call(conn, _opts) do
    case external_id(conn) do
      {:ok, external_id} -> Logger.metadata(external_id: external_id, project: external_id)
      :error -> :ok
    end

    conn
  end

  defp external_id(%{path_info: ["api", "tenants", tenant_id | _]}), do: {:ok, tenant_id}

  defp external_id(%{path_info: ["api", "ping"], host: host}), do: Database.get_external_id(host)
  defp external_id(%{path_info: ["api", "broadcast"], host: host}), do: Database.get_external_id(host)

  defp external_id(%{path_info: ["api", "broadcast", _topic, "events", _event], host: host}),
    do: Database.get_external_id(host)

  defp external_id(_conn), do: :error
end
