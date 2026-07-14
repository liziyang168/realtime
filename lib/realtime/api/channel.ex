defmodule Realtime.Api.Channel do
  @moduledoc """
  Defines the Channel config keyed by topic name.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @schema_prefix "realtime"

  @typedoc """
  * `topic` - the topic this config row applies to.
  * `broadcast_storage_enabled_at` - when broadcasts sent over WebSocket/REST on this topic started being persisted to `realtime.messages`, or `nil` if they aren't.
  """
  @type t :: %__MODULE__{
          topic: String.t(),
          broadcast_storage_enabled_at: NaiveDateTime.t() | nil
        }
  @timestamps_opts [type: :naive_datetime_usec]
  schema "channels" do
    field :topic, :string
    field :broadcast_storage_enabled_at, :naive_datetime_usec
    timestamps()
  end

  def changeset(channel, attrs) do
    channel
    |> cast(attrs, [:topic, :broadcast_storage_enabled_at])
    |> validate_required([:topic])
  end
end
