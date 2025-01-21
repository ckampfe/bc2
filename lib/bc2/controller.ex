defmodule Bc2.Controller do
  @moduledoc false

  use GenServer

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def register_keydir(directory, table) do
    :ets.insert_new(:bc2_metatable, {directory, table})
  end

  def fetch_keydir(directory) do
    case :ets.lookup(:bc2_metatable, directory) do
      [{^directory, table}] -> {:ok, table}
      [] -> :error
    end
  end

  def delete_keydir(directory) do
    :ets.delete(:bc2_metatable, directory)
  end

  def database_file(directory, file_id) do
    Path.join(directory, "#{file_id}.bc2")
  end

  @impl GenServer
  def init(args) do
    :ets.new(:bc2_metatable, [:public, :set, :named_table, read_concurrency: true])
    {:ok, args}
  end
end
