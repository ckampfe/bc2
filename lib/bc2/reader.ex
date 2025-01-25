defmodule Bc2.Reader do
  @moduledoc false

  alias Bc2.{Fs, Keydir, MetaTable}

  def fetch(directory, key) do
    with {_, {:ok, keydir_table}} <- {:fetch_keydir, MetaTable.fetch_keydir(directory)},
         {_, [{^key, file_id, value_size, entry_position, _timestamp}]} <-
           {:key_lookup, Keydir.fetch(keydir_table, key)},
         {_, {:ok, file}} <-
           {:file_open,
            :file.open(MetaTable.database_file(directory, file_id), [:raw, :read, :binary])},
         {_, {:ok, _value} = reply} <-
           {:fs_read, Fs.read(file, entry_position, key, value_size)},
         :ok <- :file.close(file) do
      reply
    else
      {:key_lookup, []} ->
        {:error, :not_found}

      {:file_open, e} ->
        e

      {:fs_read, e} ->
        e
    end
  end

  def keys(directory) do
    with {_, {:ok, keydir_table}} <- {:fetch_keydir, MetaTable.fetch_keydir(directory)} do
      Keydir.keys(keydir_table)
    end
  end
end
