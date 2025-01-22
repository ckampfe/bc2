defmodule LoadingTest do
  use ExUnit.Case, async: true
  doctest Bc2

  setup do
    Temp.track!()
    {:ok, tmp_path} = Temp.mkdir()
    [tmp_path: tmp_path]
  end

  # this is a simulation-style test
  # that performs a large number of random operations on a db
  # and then asserts that that db is in the expected state
  test "loads data", context do
    dir = context[:tmp_path]

    operations = [
      :put,
      :put,
      :put,
      :put,
      :put,
      :put,
      :delete,
      :delete,
      :close_and_reopen,
      :close_and_reopen
    ]

    number_of_operations = 1000

    :ok = Bc2.new(dir)

    state = %{expected: %{}}

    state =
      Enum.reduce(1..number_of_operations, state, fn _i, state ->
        op = Enum.random(operations)

        case op do
          # new or existing key
          :put ->
            # new key
            if :rand.uniform() >= 0.5 do
              key = :rand.bytes(20)
              value = :rand.bytes(20)

              :ok = Bc2.put(dir, key, value)

              Kernel.put_in(state, [:expected, key], value)
            else
              if Enum.empty?(state.expected) do
                state
              else
                key = Enum.random(Map.keys(state.expected))
                value = :rand.bytes(20)

                :ok = Bc2.put(dir, key, value)

                Kernel.put_in(state, [:expected, key], value)
              end
            end

          # existing key
          :delete ->
            if Enum.empty?(state.expected) do
              state
            else
              key = Enum.random(Map.keys(state.expected))

              :ok = Bc2.delete(dir, key)

              Map.update!(state, :expected, fn expected ->
                Map.delete(expected, key)
              end)
            end

          :close_and_reopen ->
            :ok = Bc2.close(dir)
            :ok = Bc2.new(dir)
            state
        end
      end)

    assert map_size(state.expected) == Enum.count(Bc2.keys(dir))
    assert MapSet.new(Map.keys(state.expected)) == MapSet.new(Bc2.keys(dir))

    Enum.each(state.expected, fn {key, expected_value} ->
      assert {:ok, ^expected_value} = Bc2.fetch(dir, key)
    end)
  end
end
