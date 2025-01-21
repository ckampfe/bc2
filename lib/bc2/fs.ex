defmodule Bc2.Fs do
  alias Bc2.Controller

  @moduledoc false

  @crc_size 4
  @timestamp_size 8
  @key_size_size 4
  @value_size_size 4
  @prefix_size @crc_size + @timestamp_size + @key_size_size + @value_size_size

  def prefix_size do
    @prefix_size
  end

  @doc """
  `file` is a file as opened by :file.open.
  `entry_position` is the byte position of the *start* of the entry in the file, not its value
  `value_size` is the size of the value, in bytes
  """
  def read(file, entry_position, key, value_size) do
    key_size =
      key
      |> encode()
      |> byte_size()

    value_position = entry_position + @prefix_size + key_size

    with {:ok, encoded_value} <- :file.pread(file, value_position, value_size) do
      {:ok, decode(encoded_value)}
    end
  end

  def write(file, key, value) do
    timestamp =
      DateTime.utc_now()
      |> DateTime.to_unix(:millisecond)

    encoded_timestamp =
      encode_u64(timestamp)

    encoded_key = encode(key)
    key_size = byte_size(encoded_key)
    encoded_key_size = encode_u32(key_size)

    encoded_value = encode(value)
    value_size = byte_size(encoded_value)
    encoded_value_size = encode_u32(value_size)

    payload = [
      encoded_timestamp,
      encoded_key_size,
      encoded_value_size,
      encoded_key,
      encoded_value
    ]

    encoded_crc =
      payload
      |> :erlang.crc32()
      |> encode_u32()

    case :file.write(file, [encoded_crc, payload]) do
      :ok ->
        {:ok, %{key_size: key_size, value_size: value_size, timestamp: timestamp}}

      e ->
        e
    end
  end

  def load_keydir_from_files(directory) do
    db_files =
      directory
      |> Path.join("*.bc2")
      |> Path.wildcard()
      |> Enum.sort_by(fn match ->
        name = Path.basename(match, ".bc2")
        {name_as_integer, _} = Integer.parse(name)
        name_as_integer
      end)

    max_id =
      Enum.map(db_files, fn match ->
        name = Path.basename(match, ".bc2")
        {name_as_integer, _} = Integer.parse(name)
        name_as_integer
      end)
      |> Enum.max(&>=/2, fn -> 0 end)

    {:ok, keydir_table} = Controller.fetch_keydir(directory)

    :ok =
      Enum.each(db_files, fn db_file ->
        :ok =
          load_records(db_file, fn record ->
            :ets.insert(
              keydir_table,
              record
            )
          end)
      end)

    {:ok, max_id}
  end

  def load_records(file_path, f) do
    {:ok, file} = :file.open(file_path, [:raw, :read, :binary])
    file_id = Path.basename(file_path, ".bc2")
    {file_id, _} = Integer.parse(file_id)
    do_load_records(file, file_id, 0, f)
  end

  defp do_load_records(file, file_id, position, f) do
    case :file.pread(file, position, prefix_size()) do
      {:ok,
       <<
         _crc::binary-size(4),
         timestamp::unsigned-integer-64,
         key_size::unsigned-integer-32,
         value_size::unsigned-integer-32
       >>} ->
        {:ok, <<key::binary-size(key_size)>>} =
          :file.pread(file, position + prefix_size(), key_size)

        f.({decode(key), file_id, value_size, position, timestamp})

        do_load_records(file, file_id, position + prefix_size() + key_size + value_size, f)

      :eof ->
        :file.close(file)
        :ok

      {:error, _} = e ->
        e
    end
  end

  defp encode(term) do
    :erlang.term_to_binary(term)
  end

  def decode(binary) do
    :erlang.binary_to_term(binary)
  end

  defp encode_u32(n) do
    <<n::unsigned-32>>
  end

  defp encode_u64(n) do
    <<n::unsigned-64>>
  end
end
