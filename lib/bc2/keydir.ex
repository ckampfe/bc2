defmodule Bc2.Keydir do
  @moduledoc false

  def new() do
    :ets.new(:bc2_keydir, [:public, :set, read_concurrency: true])
  end

  def insert(keydir_table, key, file_id, value_size, entry_position, timestamp) do
    insert(keydir_table, {key, file_id, value_size, entry_position, timestamp})
  end

  def insert(keydir_table, {_key, _file_id, _value_size, _entry_position, _timestamp} = record) do
    :ets.insert(keydir_table, record)
  end

  def fetch(keydir_table, key) do
    :ets.lookup(keydir_table, key)
  end

  def delete(keydir_table, key) do
    :ets.delete(keydir_table, key)
  end

  def keys(keydir_table) do
    :ets.select(keydir_table, [{{:"$1", :_, :_, :_, :_}, [], [:"$1"]}])
  end
end
