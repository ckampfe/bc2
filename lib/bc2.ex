# TODO
# - [x] load database files on writer start to create keydir
# - [ ] merge
# - [ ] configure max file size
# - [x] have the writer process read the existing database files and create a unique file
# - [ ] discard partial power-outage writes
# - [ ] should database load and merge processes start from the oldest or the newest db file?

defmodule Bc2 do
  alias Bc2.{DatabasesSupervisor, MetaTable, Reader, Writer}

  @doc """
  Create a new database or open it if it exists.

  Blocks the calling process.
  """
  def new(directory) when is_binary(directory) do
    new(directory, [])
  end

  def new(directory, options) when is_binary(directory) and is_list(options) do
    options = Keyword.put(options, :directory, directory)
    DatabasesSupervisor.start_writer(options)
  end

  @doc """
  Retrieves the value for a `key`, or returns an error
  if the database does not contain `key`.

  Blocks the calling process, but you can get
  read parallelism by spawning processes to call this function.
  """
  def fetch(directory, key) when is_binary(directory) do
    Reader.fetch(directory, key)
  end

  @doc """
  Store a `value` on disk that can be retrieved by `key`.

  Calls to this function are serialized through a single process.

  Blocks the calling process.
  """
  def put(directory, key, value) when is_binary(directory) do
    Writer.write(directory, key, value)
  end

  @doc """
  Mark a value as deleted, and remove key.
  Subsequent calls to `fetch` with `key` will return
  a `:not_found` error.

  Calls to this function are serialized through a single process.

  Blocks the calling process.
  """
  def delete(directory, key) when is_binary(directory) do
    Writer.delete(directory, key)
  end

  @doc """
  Return a list of all keys in the system.

  Blocks the calling process.
  """
  def keys(directory) when is_binary(directory) do
    Reader.keys(directory)
  end

  def merge(directory) when is_binary(directory) do
    # TODO
  end

  @doc """
  Flush any pending writes to disk.
  See [:file.sync/1](https://www.erlang.org/docs/19/man/file#sync-1).

  Calls to this function are serialized through a single process.

  Blocks the calling process.
  """
  def sync(directory) when is_binary(directory) do
    Writer.sync(directory)
  end

  @doc """
  Flushes any pending writes to disk,
  stops the writer process, and deletes the keydir from memory.

  Calls to this function are serialized through a single process.

  Blocks the calling process.
  """
  def close(directory) when is_binary(directory) do
    with {_, :ok} <- {:sync, sync(directory)},
         {_, true} <- {:delete_keydir, MetaTable.delete_keydir(directory)},
         {_, :ok} <- {:stop_writer, DatabasesSupervisor.stop_writer(directory)} do
      :ok
    end
  end
end
