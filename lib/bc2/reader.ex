defmodule Bc2.Reader do
  @moduledoc false

  alias Bc2.{Controller, Fs}

  def fetch(directory, key) do
    with {_, {:ok, keydir_table}} <- {:fetch_keydir, Controller.fetch_keydir(directory)},
         {_, [{^key, file_id, value_size, entry_position, _timestamp}]} <-
           {:key_lookup, :ets.lookup(keydir_table, key)},
         {_, {:ok, file}} <-
           {:file_open,
            :file.open(Controller.database_file(directory, file_id), [:raw, :read, :binary])},
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
    with {_, {:ok, keydir_table}} <- {:fetch_keydir, Controller.fetch_keydir(directory)} do
      :ets.select(keydir_table, [{{:"$1", :_, :_, :_, :_}, [], [:"$1"]}])
    end
  end
end
