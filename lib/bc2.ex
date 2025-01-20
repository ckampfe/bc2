# TODO
# - [ ] merge
# - [ ] configure max file size

defmodule Bc2 do
  alias Bc2.{Controller, DatabasesSupervisor, Reader, Writer}

  def new(directory) when is_binary(directory) do
    new(directory, [])
  end

  @doc """
  options:
  - directory (required)
  """
  def new(directory, options) when is_binary(directory) and is_list(options) do
    options = Keyword.put(options, :directory, directory)
    DatabasesSupervisor.start_writer(options)
  end

  def fetch(directory, key) when is_binary(directory) do
    Reader.fetch(directory, key)
  end

  def put(directory, key, value) when is_binary(directory) do
    Writer.write(directory, key, value)
  end

  def delete(directory, key) when is_binary(directory) do
    Writer.delete(directory, key)
  end

  def keys(directory) when is_binary(directory) do
    Reader.keys(directory)
  end

  def merge(directory) when is_binary(directory) do
    # TODO
  end

  def sync(directory) when is_binary(directory) do
    Writer.sync(directory)
  end

  def close(directory) when is_binary(directory) do
    with {_, :ok} <- {:sync, sync(directory)},
         {_, true} <- {:delete_keydir, Controller.delete_keydir(directory)},
         {_, :ok} <- {:stop_writer, DatabasesSupervisor.stop_writer(directory)} do
      :ok
    end
  end
end
