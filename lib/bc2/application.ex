defmodule Bc2.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {PartitionSupervisor, child_spec: Bc2.Reader.child_spec([]), name: Bc2.PartitionSupervisor},
      {Bc2.DatabasesSupervisor, %{}}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Bc2.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
