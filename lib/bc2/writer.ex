defmodule Bc2.Writer do
  @moduledoc false

  use GenServer
  alias Bc2.{Controller, Fs}

  defstruct [:table, :directory, :file_id, :file, :entry_position]

  def start_link(args) do
    GenServer.start_link(
      __MODULE__,
      args,
      name: name(args[:directory])
    )
  end

  def write(directory, key, value) do
    GenServer.call(name(directory), {:write, key, value})
  end

  def delete(directory, key) do
    GenServer.call(name(directory), {:delete, key})
  end

  def sync(directory) do
    GenServer.call(name(directory), :sync)
  end

  def init(args) do
    {:ok, %__MODULE__{directory: args[:directory]}, {:continue, :initialize_table}}
  end

  def handle_continue(:initialize_table, %__MODULE__{} = state) do
    table = :ets.new(:bc2_keydir, [:public, :set, read_concurrency: true])

    true = Controller.register_keydir(state.directory, table)

    # TODO make this truly random/unique so it doesn't class with exist files
    file_id = System.unique_integer([:positive, :monotonic])

    path = Controller.database_file(state.directory, file_id)

    {:ok, file} = :file.open(path, [:read, :append, :raw, :binary])

    state = %{state | file: file, table: table, file_id: file_id, entry_position: 0}

    {:noreply, state}
  end

  def handle_call({:delete, key}, _from, state) do
    {:ok, %{key_size: key_size, value_size: value_size}} =
      Fs.write(state.file, key, :bc2_delete)

    :ets.delete(state.table, key)

    state = %{
      state
      | entry_position: state.entry_position + Fs.prefix_size() + key_size + value_size
    }

    {:reply, :ok, state}
  end

  def handle_call({:write, key, value}, _from, %__MODULE__{} = state) do
    {:ok, %{key_size: key_size, value_size: value_size, timestamp: timestamp}} =
      Fs.write(state.file, key, value)

    :ets.insert(state.table, {key, state.file_id, value_size, state.entry_position, timestamp})

    state = %{
      state
      | entry_position: state.entry_position + Fs.prefix_size() + key_size + value_size
    }

    {:reply, :ok, state}
  end

  def handle_call(:sync, _from, state) do
    {:reply, :file.sync(state.file), state}
  end

  def name(directory) do
    {:via, Registry, {Bc2.Registry, {__MODULE__, directory}}}
  end

  def pid(directory) do
    case Registry.lookup(Bc2.Registry, {__MODULE__, directory}) do
      [{pid, _}] -> pid
      _ -> nil
    end
  end
end
