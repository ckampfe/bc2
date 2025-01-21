# Bc2

[![Elixir CI](https://github.com/ckampfe/bc2/actions/workflows/elixir.yml/badge.svg)](https://github.com/ckampfe/bc2/actions/workflows/elixir.yml)

Yet another [Bitcask](https://en.wikipedia.org/wiki/Bitcask) implementation.

## What

[The paper](bitcask-intro.pdf) describes Bitcask as "A Log-Structured Hash Table for Fast Key/Value Data".

In other words, Bitcask is a key-value database where values are appended to files on disk,
and an in-memory hash table contains pointers to the locations of those on-disk values.

This conceptual model has some compelling attributes:
- it is simple to understand (the paper is very short)
- it is simple to implement (really: go look at the code)
- it has low read and write latency
- its performance is predictable
- readers and writers do not block each other

## Example

```elixir
directory = "."
# open a new (or existing) database
:ok = Bc2.new(directory)

# store a key-value pair
:ok = Bc2.put(directory, :hello, :world)

# read a value for a given key
{:ok, :world} = Bc2.fetch(directory, :hello)

# delete the value stored at a key
:ok = Bc2.delete(directory, :hello)

# the key (and value) are deleted
{:error, :not_found} = Bc2.fetch(directory, :hello)
```

## A little more detail

Bitcask works by storing the location of on-disk key-value pairs in an in-memory hash table.

For reads, you have a key "hello".
You want to know what value "hello" points to.
You look up "hello" in the in-memory hash table.
It gives you a file and a byte location in that file.
You read those bytes from that file and they say "world".

For writes, you want to store the value "world" associated with the key "hello".
You append "world" and some metadata to a file.
When appending to that file completes, you insert the file id and the byte location of that
append in that file into the in-memory hash table at the key "hello".

Deletes are just like writes, but you write a special tombstone value instead of a real value.

In Bitcask, all disk write operations are append-only, so files only grow.
This is not ideal long term, so there is a special merge process
that goes through all database files and writes new files that only contain
live data.

There's more to it, but that's basically how reads, writes, and deletes work in Bitcask.

See [the paper](bitcask-intro.pdf).
