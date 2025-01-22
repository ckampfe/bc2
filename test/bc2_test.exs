defmodule Bc2Test do
  use ExUnit.Case, async: true
  doctest Bc2

  setup do
    Temp.track!()
    {:ok, tmp_path} = Temp.mkdir()
    [tmp_path: tmp_path]
  end

  test "roundtrips data", context do
    dir = context[:tmp_path]
    assert :ok = Bc2.new(dir)

    assert :ok = Bc2.put(dir, :hello, :world)
    assert {:ok, :world} = Bc2.fetch(dir, :hello)
  end

  test "no key", context do
    dir = context[:tmp_path]
    assert :ok = Bc2.new(dir)

    assert :ok = Bc2.put(dir, :hello, :world)
    assert {:error, :not_found} = Bc2.fetch(dir, :no_key)
  end

  test "overwrites same key", context do
    dir = context[:tmp_path]
    assert :ok = Bc2.new(dir)

    assert :ok = Bc2.put(dir, :hello, :world)
    assert {:ok, :world} = Bc2.fetch(dir, :hello)

    assert :ok = Bc2.put(dir, :hello, :world2)
    assert {:ok, :world2} = Bc2.fetch(dir, :hello)
  end

  test "multiple different keys", context do
    dir = context[:tmp_path]
    assert :ok = Bc2.new(dir)

    assert :ok = Bc2.put(dir, :hello, :world)
    assert {:ok, :world} = Bc2.fetch(dir, :hello)

    assert :ok = Bc2.put(dir, :hi, :mom)
    assert {:ok, :mom} = Bc2.fetch(dir, :hi)

    assert :ok = Bc2.put(dir, :hello, :world2)
    assert {:ok, :world2} = Bc2.fetch(dir, :hello)
  end

  test "delete/2 found key", context do
    dir = context[:tmp_path]
    assert :ok = Bc2.new(dir)

    assert :ok = Bc2.put(dir, :hello, :world)
    assert {:ok, :world} = Bc2.fetch(dir, :hello)

    assert :ok = Bc2.delete(dir, :hello)
    assert {:error, :not_found} = Bc2.fetch(dir, :hello)
  end

  test "delete/2 no key", context do
    dir = context[:tmp_path]
    assert :ok = Bc2.new(dir)

    assert :ok = Bc2.delete(dir, :hello)
    assert {:error, :not_found} = Bc2.fetch(dir, :hello)
  end

  test "sync/1", context do
    dir = context[:tmp_path]
    assert :ok = Bc2.new(dir)

    assert :ok = Bc2.put(dir, :hello, :world)
    assert :ok = Bc2.sync(dir)
    assert {:ok, :world} = Bc2.fetch(dir, :hello)
  end

  test "close/1", context do
    dir = context[:tmp_path]
    assert :ok = Bc2.new(dir)

    assert :ok = Bc2.close(dir)
  end

  test "keys/1", context do
    dir = context[:tmp_path]
    assert :ok = Bc2.new(dir)

    assert [] = Bc2.keys(dir)

    assert :ok = Bc2.put(dir, :a, :world)
    assert [:a] = Bc2.keys(dir)

    assert :ok = Bc2.put(dir, :b, :world)
    assert [:b, :a] = Bc2.keys(dir)

    assert :ok = Bc2.put(dir, :c, :world)
    assert [:c, :b, :a] = Bc2.keys(dir)
  end

  test "load files in directory on start", context do
    dir = context[:tmp_path]

    assert :ok = Bc2.new(dir)
    assert :ok = Bc2.put(dir, :a, :world1)
    assert :ok = Bc2.put(dir, :b, :world2)
    assert :ok = Bc2.put(dir, :c, :world3)
    assert {:ok, :world1} = Bc2.fetch(dir, :a)

    assert :ok = Bc2.close(dir)

    assert :ok = Bc2.new(dir)

    assert {:ok, :world1} = Bc2.fetch(dir, :a)
    assert {:ok, :world2} = Bc2.fetch(dir, :b)
    assert {:ok, :world3} = Bc2.fetch(dir, :c)
  end

  test "does not load deletes", context do
    dir = context[:tmp_path]

    assert :ok = Bc2.new(dir)
    assert :ok = Bc2.put(dir, :a, :world1)
    assert :ok = Bc2.put(dir, :b, :world2)

    assert {:ok, :world1} = Bc2.fetch(dir, :a)
    assert {:ok, :world2} = Bc2.fetch(dir, :b)

    # now delete :a
    assert :ok = Bc2.delete(dir, :a)

    assert :ok = Bc2.close(dir)

    assert :ok = Bc2.new(dir)

    assert {:error, :not_found} = Bc2.fetch(dir, :a)
    assert {:ok, :world2} = Bc2.fetch(dir, :b)

    assert [:b] = Bc2.keys(dir)
  end

  test "allows multiple databases to be open concurrently", context do
    dir1 = context[:tmp_path]
    {:ok, dir2} = Temp.mkdir()

    assert :ok = Bc2.new(dir1)
    assert :ok = Bc2.put(dir1, :a, :world1)
    assert {:ok, :world1} = Bc2.fetch(dir1, :a)

    assert :ok = Bc2.new(dir2)
    assert :ok = Bc2.put(dir2, :b, :world2)
    assert {:ok, :world2} = Bc2.fetch(dir2, :b)

    # they do not contain each other's keys
    assert {:error, :not_found} = Bc2.fetch(dir1, :b)
    assert {:error, :not_found} = Bc2.fetch(dir2, :a)
  end
end
