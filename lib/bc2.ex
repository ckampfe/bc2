# architecture to investigate:
# - single writer process
# - N reader processes, partitioned by PartitionSupervisor
# - call read/write/delete with some opaque handle, API does the right thing
#
# we want to have writes via a single process
# we want up to some N number of concurrent readers

defmodule Bc2 do
  def new(options) do
    Bc2.Keydir.start_link(options)
  end

  def fetch(handle, key) do
    Bc2.Keydir.read(handle, key)
  end

  def put(handle, key, value) do
    Bc2.Keydir.write(handle, key, value)
  end

  def delete(handle, key) do
    Bc2.Keydir.delete(handle, key)
  end

  def keys(_handle) do
    # TODO
  end

  def merge(_handle) do
    # TODO
  end

  def sync(handle) do
    Bc2.Keydir.sync(handle)
  end

  def close(_handle) do
    # TODO
  end
end
