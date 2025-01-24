defmodule Gfs.ClientTest do
  use ExUnit.Case
  doctest Gfs.Client

  test "greets the world" do
    assert Gfs.Client.hello() == :world
  end
end
