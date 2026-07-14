defmodule RealtimeWeb.RealtimeChannel.BroadcastHandler do
  @moduledoc """
  Handles the Broadcast feature from Realtime
  """
  use Realtime.Logs

  import Phoenix.Socket, only: [assign: 3]

  alias Realtime.Messages
  alias Realtime.Tenants
  alias RealtimeWeb.RealtimeChannel
  alias RealtimeWeb.TenantBroadcaster
  alias Phoenix.Socket
  alias Realtime.GenCounter
  alias Realtime.Tenants.Authorization
  alias Realtime.Tenants.Authorization.Policies
  alias Realtime.Tenants.Authorization.Policies.BroadcastPolicies

  @type payload :: map | {String.t(), :json | :binary, binary}

  @event_type "broadcast"

  @spec handle(payload, pid() | nil, Socket.t()) ::
          {:reply, :ok, Socket.t()}
          | {:reply, {:ok, map()}, Socket.t()}
          | {:reply, {:error, any()}, Socket.t()}
          | {:noreply, Socket.t()}
  def handle(payload, db_conn, %{assigns: %{private?: true}} = socket) do
    %{
      assigns: %{
        self_broadcast: self_broadcast,
        tenant_topic: tenant_topic,
        authorization_context: authorization_context,
        policies: policies,
        tenant: tenant_id,
        ack_broadcast: ack_broadcast
      }
    } = socket

    with {:ok, %Policies{broadcast: %BroadcastPolicies{write: true}} = policies} <-
           run_authorization_check(policies || %Policies{}, db_conn, authorization_context),
         socket = socket |> assign(:policies, policies) |> increment_rate_counter(),
         :ok <- Tenants.validate_payload_size(tenant_id, payload) do
      # Store before broadcasting to permit recovering/replaying messages in case of failure.
      store_result =
        case convert_to_storable_fields(payload) do
          {:ok, event, event_payload} ->
            Messages.store(db_conn, tenant_id, authorization_context.topic, event, event_payload, true)

          :error ->
            :skip
        end

      send_message(tenant_id, self_broadcast, tenant_topic, payload)

      cond do
        store_result == {:error, :storage_disabled} ->
          if ack_broadcast, do: {:reply, :ok, socket}, else: {:noreply, socket}

        match?({:error, _reason}, store_result) ->
          {:error, reason} = store_result
          log_error("UnableToStoreBroadcast", reason)
          if ack_broadcast, do: {:reply, {:error, %{reason: "unable_to_store"}}, socket}, else: {:noreply, socket}

        ack_broadcast and match?({:ok, _id}, store_result) ->
          {:ok, id} = store_result
          {:reply, {:ok, %{id: id}}, socket}

        ack_broadcast ->
          {:reply, :ok, socket}

        true ->
          {:noreply, socket}
      end
    else
      {:ok, policies} ->
        {:noreply, assign(socket, :policies, policies)}

      {:error, :payload_size_exceeded} ->
        if ack_broadcast, do: {:reply, {:error, :payload_size_exceeded}, socket}, else: {:noreply, socket}

      {:error, :rls_policy_error, error} ->
        log_error("RlsPolicyError", error)
        {:noreply, socket}

      {:error, :query_canceled, error} ->
        log_error("QueryCanceled", error)
        {:noreply, socket}

      {:error, :missing_partition} ->
        log_error("MissingPartition", "Realtime was unable to find the expected messages partition")
        {:noreply, socket}

      {:error, :tenant_database_unavailable} ->
        log_error("UnableToConnectToProject", "Realtime was unable to connect to the project database")
        {:noreply, socket}

      {:error, :increase_connection_pool} ->
        {:noreply, socket}

      {:error, error} ->
        log_error("UnableToSetPolicies", error)
        {:noreply, socket}
    end
  end

  def handle(payload, _db_conn, %{assigns: %{private?: false}} = socket) do
    %{
      assigns: %{
        tenant_topic: tenant_topic,
        self_broadcast: self_broadcast,
        ack_broadcast: ack_broadcast,
        tenant: tenant_id,
        authorization_context: authorization_context
      }
    } = socket

    socket = increment_rate_counter(socket)

    case Tenants.validate_payload_size(tenant_id, payload) do
      :ok ->
        send_message(tenant_id, self_broadcast, tenant_topic, payload)

        case convert_to_storable_fields(payload) do
          {:ok, event, event_payload} ->
            Messages.store_async(tenant_id, authorization_context.topic, event, event_payload)

          :error ->
            :ok
        end

        if ack_broadcast, do: {:reply, :ok, socket}, else: {:noreply, socket}

      {:error, :payload_size_exceeded} ->
        if ack_broadcast,
          do: {:reply, {:error, :payload_size_exceeded}, socket},
          else: {:noreply, socket}
    end
  end

  defp send_message(tenant_id, self_broadcast, tenant_topic, payload) do
    broadcast = build_broadcast(tenant_topic, payload)

    if self_broadcast do
      TenantBroadcaster.pubsub_broadcast(
        tenant_id,
        tenant_topic,
        broadcast,
        RealtimeChannel.MessageDispatcher,
        :broadcast
      )
    else
      TenantBroadcaster.pubsub_broadcast_from(
        tenant_id,
        self(),
        tenant_topic,
        broadcast,
        RealtimeChannel.MessageDispatcher,
        :broadcast
      )
    end
  end

  # No idea why Dialyzer is complaining here
  @dialyzer {:nowarn_function, build_broadcast: 2}

  # Message payload was built by V2 Serializer which was originally UserBroadcastPush
  # We are not using the metadata for anything just yet.
  defp build_broadcast(topic, {user_event, user_payload_encoding, user_payload, _metadata}) do
    %RealtimeWeb.Socket.UserBroadcast{
      topic: topic,
      user_event: user_event,
      user_payload_encoding: user_payload_encoding,
      user_payload: user_payload
    }
  end

  defp build_broadcast(topic, payload) do
    %Phoenix.Socket.Broadcast{topic: topic, event: @event_type, payload: payload}
  end

  # Same two payload shapes `build_broadcast/2` branches on above, but for storage: only the
  # JSON-protocol map shape carries an event/payload that fits `realtime.messages.payload`
  # (jsonb). The V2 binary protocol's `user_payload` is an arbitrary client-chosen binary blob,
  # not necessarily even valid JSON, so there's nothing storable to extract from it yet.
  defp convert_to_storable_fields(%{"event" => event, "payload" => payload}), do: {:ok, event, payload}
  defp convert_to_storable_fields(_payload), do: :error

  defp increment_rate_counter(%{assigns: %{policies: %Policies{broadcast: %BroadcastPolicies{write: false}}}} = socket) do
    socket
  end

  defp increment_rate_counter(%{assigns: %{rate_counter: counter}} = socket) do
    GenCounter.add(counter.id)
    socket
  end

  defp run_authorization_check(
         %Policies{broadcast: %BroadcastPolicies{write: nil}} = policies,
         db_conn,
         authorization_context
       ) do
    Authorization.get_write_authorizations(policies, db_conn, authorization_context)
  end

  defp run_authorization_check(socket, _db_conn, _authorization_context) do
    {:ok, socket}
  end
end
