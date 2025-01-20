defmodule Bc2Test do
  use ExUnit.Case
  doctest Bc2

  setup do
    Temp.track!()
    {:ok, tmp_path} = Temp.mkdir()
    [tmp_path: tmp_path]
  end

  test "roundtrips data", context do
    {:ok, server} = Bc2.new(dir: context[:tmp_path])

    assert :ok = Bc2.put(server, :hello, :world)
    assert {:ok, :world} = Bc2.fetch(server, :hello)
  end

  test "no key", context do
    {:ok, server} = Bc2.new(dir: context[:tmp_path])

    assert :ok = Bc2.put(server, :hello, :world)
    assert {:error, :not_found} = Bc2.fetch(server, :no_key)
  end

  test "overwrites same key", context do
    {:ok, server} = Bc2.new(dir: context[:tmp_path])

    assert :ok = Bc2.put(server, :hello, :world)
    assert {:ok, :world} = Bc2.fetch(server, :hello)

    assert :ok = Bc2.put(server, :hello, :world2)
    assert {:ok, :world2} = Bc2.fetch(server, :hello)
  end

  test "multiple different keys", context do
    {:ok, server} = Bc2.new(dir: context[:tmp_path])

    assert :ok = Bc2.put(server, :hello, :world)
    assert {:ok, :world} = Bc2.fetch(server, :hello)

    assert :ok = Bc2.put(server, :hi, :mom)
    assert {:ok, :mom} = Bc2.fetch(server, :hi)

    assert :ok = Bc2.put(server, :hello, :world2)
    assert {:ok, :world2} = Bc2.fetch(server, :hello)
  end

  test "delete/2 found key", context do
    {:ok, server} = Bc2.new(dir: context[:tmp_path])

    assert :ok = Bc2.put(server, :hello, :world)
    assert {:ok, :world} = Bc2.fetch(server, :hello)

    assert :ok = Bc2.delete(server, :hello)
    assert {:error, :not_found} = Bc2.fetch(server, :hello)
  end

  test "delete/2 no key", context do
    {:ok, server} = Bc2.new(dir: context[:tmp_path])

    assert :ok = Bc2.delete(server, :hello)
    assert {:error, :not_found} = Bc2.fetch(server, :hello)
  end

  test "sync/1", context do
    {:ok, server} = Bc2.new(dir: context[:tmp_path])

    assert :ok = Bc2.put(server, :hello, :world)
    assert :ok = Bc2.sync(server)
    assert {:ok, :world} = Bc2.fetch(server, :hello)
  end
end
