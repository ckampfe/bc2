defmodule Bc2.Reader do
  use GenServer
  alias Bc2.Fs

  defstruct [:table, :dir, :file_id, :file, :entry_position]

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  def read(server, key) do
    GenServer.call(server, {:read, key})
  end

  def init(args) do
    {:ok, %__MODULE__{dir: args[:dir]}, {:continue, :initialize_table}}
  end

  def handle_continue(:initialize_table, %__MODULE__{} = state) do
    table = :ets.new(:bc2_keydir, [:public, :set, read_concurrency: true])
    # TODO make this truly random/unique so it doesn't class with exist files
    file_id = System.unique_integer([:positive, :monotonic])

    path = Path.join(state.dir, "#{file_id}.bc2")

    {:ok, file} = :file.open(path, [:read, :append, :raw, :binary])

    state = %{state | file: file, table: table, file_id: file_id, entry_position: 0}

    {:noreply, state}
  end

  def handle_call({:read, key}, _from, %__MODULE__{file_id: file_id} = state) do
    with {_, [{^key, ^file_id, value_size, entry_position, _timestamp}]} <-
           {:key_lookup, :ets.lookup(state.table, key)},
         {_, {:ok, _value} = reply} <-
           {:fs_read, Fs.read(state.file, entry_position, key, value_size)} do
      {:reply, reply, state}
    else
      {:key_lookup, []} ->
        {:reply, {:error, :not_found}, state}

      {:fs_read, error} ->
        {:reply, error, state}
    end
  end
end
