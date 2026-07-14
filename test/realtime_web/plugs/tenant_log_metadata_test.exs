defmodule RealtimeWeb.Plugs.TenantLogMetadataTest do
  use ExUnit.Case, async: true
  import Plug.Test

  alias RealtimeWeb.Plugs.TenantLogMetadata

  defp call(conn) do
    TenantLogMetadata.call(conn, TenantLogMetadata.init([]))
  end

  test "tags metadata from the host for tenant-scoped api routes" do
    conn = %{conn(:get, "/api/ping") | host: "my_tenant.localhost.com"}
    call(conn)

    assert Logger.metadata()[:external_id] == "my_tenant"
    assert Logger.metadata()[:project] == "my_tenant"
  end

  test "tags metadata from the path for tenant management routes" do
    conn = conn(:get, "/api/tenants/my_tenant/health")
    call(conn)

    assert Logger.metadata()[:external_id] == "my_tenant"
    assert Logger.metadata()[:project] == "my_tenant"
  end

  test "tags metadata from the host for single broadcast routes" do
    conn = %{conn(:post, "/api/broadcast/my_topic/events/my_event") | host: "my_tenant.localhost.com"}
    call(conn)

    assert Logger.metadata()[:external_id] == "my_tenant"
    assert Logger.metadata()[:project] == "my_tenant"
  end

  test "does not tag metadata for routes without a resolvable tenant" do
    for conn <- [conn(:get, "/healthcheck"), conn(:get, "/api/tenants"), conn(:get, "/api/openapi")] do
      call(conn)

      assert Logger.metadata()[:external_id] == nil
      assert Logger.metadata()[:project] == nil
    end
  end
end
