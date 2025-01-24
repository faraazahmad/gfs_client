defmodule Gfs.Client do
  @moduledoc """
  Documentation for `Gfs.Client`.
  """

  @doc """
  Hello world.

  ## Examples

      iex> Gfs.Client.hello()
      :world

  """
  def hello do
    :world
  end

  def append_file(file_name, content) do
    # get all chunks info from master
    # Get last chunk and all its replicas
    # Assign primary to one replica
    # Append primary replica then append other secondary replicas
  end
end
