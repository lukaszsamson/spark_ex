defmodule SparkEx.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Task.Supervisor, name: SparkEx.TaskSupervisor},
      {SparkEx.EtsTableOwner, []},
      {GRPC.Client.Supervisor, []}
    ]

    opts = [strategy: :one_for_one, name: SparkEx.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
