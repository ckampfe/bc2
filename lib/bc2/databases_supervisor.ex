defmodule Bc2.DatabasesSupervisor do
  @moduledoc false

  use DynamicSupervisor

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def start_writer(options) do
    {:ok, _pid} = DynamicSupervisor.start_child(__MODULE__, {Bc2.Writer, options})
    :ok
  end

  def stop_writer(directory) do
    DynamicSupervisor.terminate_child(__MODULE__, Bc2.Writer.pid(directory))
  end
end
