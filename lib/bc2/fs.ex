defmodule Bc2.Fs do
  alias Bc2.{Keydir, MetaTable}

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

    {:ok, keydir_table} = MetaTable.fetch_keydir(directory)

    :ok =
      Enum.each(db_files, fn db_file ->
        :ok =
          load_records(db_file, fn
            {:insert, record} ->
              Keydir.insert(keydir_table, record)

            {:delete, {key, _, _, _, _}} ->
              Keydir.delete(keydir_table, key)
          end)
      end)

    {:ok, max_id}
  end

  def load_records(file_path, f) do
    {:ok, file} = :file.open(file_path, [:raw, :read, :binary])

    try do
      file_id = Path.basename(file_path, ".bc2")
      {file_id, _} = Integer.parse(file_id)
      do_load_records(file, file_id, 0, f)
    after
      :file.close(file)
    end
  end

  defp do_load_records(file, file_id, position, f) do
    case :file.pread(file, position, prefix_size()) do
      {:ok,
       <<
         crc::unsigned-integer-32,
         timestamp_encoded::binary-size(8),
         key_size_encoded::binary-size(4),
         value_size_encoded::binary-size(4)
       >>} ->
        key_size = decode_u32(key_size_encoded)
        value_size = decode_u32(value_size_encoded)

        {:ok, <<key::binary-size(key_size), value::binary-size(value_size)>>} =
          :file.pread(file, position + prefix_size(), key_size + value_size)

        challenge_crc =
          :erlang.crc32([
            timestamp_encoded,
            key_size_encoded,
            value_size_encoded,
            key,
            value
          ])

        ^crc = challenge_crc

        record = {decode(key), file_id, value_size, position, decode_u64(timestamp_encoded)}

        if decode(value) == :bc2_delete do
          f.({:delete, record})
        else
          f.({:insert, record})
        end

        do_load_records(file, file_id, position + prefix_size() + key_size + value_size, f)

      :eof ->
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

  defp decode_u32(<<n::unsigned-integer-32>>) do
    n
  end

  defp encode_u64(n) do
    <<n::unsigned-64>>
  end

  defp decode_u64(<<n::unsigned-integer-64>>) do
    n
  end
end
