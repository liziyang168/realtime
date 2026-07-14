defmodule RealtimeWeb.Plugs.TenantLogMetadataRoutesTest do
  # Walks every route declared in `RealtimeWeb.Router` and checks that
  # `RealtimeWeb.Plugs.TenantLogMetadata` tags Logger metadata for it if and only if the
  # route actually resolves a tenant (either via one of the tenant-resolving pipelines, or
  # via a `:tenant_id` path param). This stays in sync automatically as routes are added or
  # removed, instead of relying on a hand-maintained list of endpoints.
  use ExUnit.Case, async: true
  import Plug.Test

  alias RealtimeWeb.Plugs.TenantLogMetadata
  alias RealtimeWeb.Router

  @tenant_pipelines [:tenant_api, :secure_tenant_api, :broadcast_single]

  test "tags exactly the routes that resolve a tenant" do
    for route <- Router.__routes__() do
      path = concrete_path(route.path)
      method = route.verb |> Atom.to_string() |> String.upcase()

      %{pipe_through: pipe_through} = Phoenix.Router.route_info(Router, method, path, "router-host.example.com")

      expected? = String.contains?(route.path, ":tenant_id") or Enum.any?(pipe_through, &(&1 in @tenant_pipelines))

      Logger.reset_metadata(external_id: nil, project: nil)

      conn(route.verb, path)
      |> Map.put(:host, "sample_tenant.localhost.com")
      |> then(&TenantLogMetadata.call(&1, TenantLogMetadata.init([])))

      tagged? = not is_nil(Logger.metadata()[:external_id])

      assert tagged? == expected?,
             "expected tag=#{expected?} but got tag=#{tagged?} for #{route.verb} #{route.path}"
    end
  end

  defp concrete_path(path) do
    path
    |> String.split("/")
    |> Enum.map(fn
      ":" <> _ -> "sample"
      "*" <> _ -> "sample"
      segment -> segment
    end)
    |> Enum.join("/")
  end
end
