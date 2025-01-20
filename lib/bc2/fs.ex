defmodule Bc2.Fs do
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
      |> DateTime.to_unix()

    encoded_timestamp =
      encode_u64(timestamp)

    encoded_key = encode(key)
    key_size = byte_size(encoded_key)
    encoded_key_size = byte_size(encoded_key) |> encode_u32()

    encoded_value = encode(value)
    value_size = byte_size(encoded_value)
    encoded_value_size = value_size |> encode_u32()

    payload = [
      encoded_timestamp,
      encoded_key_size,
      encoded_value_size,
      encoded_key,
      encoded_value
    ]

    crc32 = :erlang.crc32(payload)
    encoded_crc = encode_u32(crc32)

    case :file.write(file, [encoded_crc, payload]) do
      :ok ->
        {:ok, %{key_size: key_size, value_size: value_size, timestamp: timestamp}}

      e ->
        e
    end
  end

  defp encode(term) do
    :erlang.term_to_binary(term)
  end

  defp decode(binary) do
    :erlang.binary_to_term(binary)
  end

  defp encode_u32(n) do
    <<n::unsigned-32>>
  end

  defp encode_u64(n) do
    <<n::unsigned-64>>
  end
end
