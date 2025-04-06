defmodule Gfs.Client do
  @moduledoc """
  Documentation for `Gfs.Client`.
  """

  @doc """
  Hello world.

  ## Examples

      iex> Gfs.Client.append_file("severance_s2e1.mp4", <<...>>)
      :world

  """
  def append_file(file_name, content) do
    # get all chunks info from master
    # Get last chunk and all its replicas
    # Assign primary to one replica
    # Append primary replica then append other secondary replicas
  end

  @doc """
  Read a file from local FS, divide it into 64Mb chunks, and send to master for writing

  ## Examples

      iex> local_path = "/mnt/d/TV Shows/Severance Season 2 Mp4 1080p/Severance S02E01.mp4"
      iex> Gfs.Client.write_file_from_local_fs(local_path, "severance/s02/ep01.mp4")

  """
  def write_file_from_local_fs(local_path, _gfs_path) do
    # Read 64KB chunks
    for_each_chunk(local_path, fn chunk, chunk_start_byte ->
        chunk_end_byte = chunk_start_byte + byte_size(chunk) - 1

        IO.puts("start_byte: #{chunk_start_byte} -- end_byte: #{chunk_end_byte}")
    end)
  end

  @doc """
  Read a file from local_fs in 64 Mb chunks

  ## Examples

      iex> local_path = "/mnt/d/TV Shows/Severance Season 2 Mp4 1080p/Severance S02E01.mp4"
      iex> Gfs.Client.read_file_in_chunks(local_path)

  """
  def for_each_chunk(file_path, chunk_size \\ 64 * 1024 * 1024, task) do
    File.stream!(file_path, chunk_size)
    |> Enum.reduce(0, fn chunk, start_byte ->
        task.(chunk, start_byte)
        start_byte + byte_size(chunk)
    end)
  end
end
