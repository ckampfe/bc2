defmodule Bc2.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Bc2.Registry},
      {Bc2.Controller, %{}},
      {Bc2.DatabasesSupervisor, %{}}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Bc2.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
