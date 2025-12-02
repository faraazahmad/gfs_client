defmodule Gfs.Client do
  @moduledoc """
  Documentation for `Gfs.Client`.
  """

  # 64 MB
  @chunk_size 64 * 1024 * 1024

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

  def write_file_from_local_fs("", _gfs_path, _force) do
    raise "Invalid local path"
  end

  def write_file_from_local_fs(_local_path, "", _force) do
    raise "Invalid gfs path"
  end

  # When force is false (prefered method) file is appended only
  def write_file_from_local_fs(local_path, gfs_path, false) do
    encoded_gfs_path = Base.encode16(gfs_path)
    # get all stored chunks for a file
    last_chunk =
      case HTTPoison.get(
             "http://localhost:4000/file/#{encoded_gfs_path}/chunks/last",
             [{"Content-Type", "application/json"}]
           ) do
        {:ok, %HTTPoison.Response{status_code: _status, body: body}} ->
          body

        {:error, error} ->
          IO.puts("#{error.reason}")
          nil
      end

    IO.puts(last_chunk)

    # Read 64KB chunks
    for_each_chunk(local_path, fn chunk, chunk_start_byte ->
      # chunk_end_byte = chunk_start_byte + byte_size(chunk) - 1
      # chunk_id = Base.encode16("#{gfs_path}:#{chunk_start_byte},#{chunk_end_byte}")
      process_file_chunk(chunk, chunk_start_byte, last_chunk)

      # case HTTPoison.get(
      #        "http://localhost:4000/file/#{encoded_gfs_path}/chunks",
      #        [{"Content-Type", "application/json"}]
      #      ) do
      #   {:ok, %HTTPoison.Response{status_code: status, body: body}} ->
      #     IO.puts("It was fine. status: #{status}, body: #{body}")
      #
      #   {:error, error} ->
      #     IO.puts(error.reason)
      # end

      # request_payload =
      #   Poison.encode!(%{
      #     content: :base64.encode(chunk),
      #     start_byte: chunk_start_byte,
      #     end_byte: chunk_end_byte,
      #     force: force
      #   })
      #
      # case HTTPoison.post(
      #        "http://localhost:4000/file/#{Base.encode16(gfs_path)}/chunk",
      #        request_payload,
      #        [{"Content-Type", "application/json"}]
      #      ) do
      #   {:ok, %HTTPoison.Response{status_code: status, body: body}} ->
      #     IO.puts("#{status}, #{body}")
      #     IO.puts("Succesfully created chunk@#{chunk_start_byte}.")
      #
      #   {:error, error} ->
      #     IO.puts("There was an error creating chunk@#{chunk_start_byte}: #{error}")
      # end
    end)
  end

  # Multiple conditions when processing file chunk:
  # 1. local_start_byte < last_remote_chunk.start_byte => cannot happen when appending, return early.
  # 2. local_start_byte == last_remote_chunk.start_byte => append the last_chunk.
  #   2.1. local_chunk.size > last_remote_chunk.size => append the last chunk.
  #   2.2. local_chunk.size == last_remote_chunk.size => No changes were made, do nothing.
  #   2.3. local_chunk.size < last_remote_chunk.size => Deleting is not allowed, return early.
  # 3. local_start_byte > last_remote_chunk.end_byte => Create new chunk and append to it.

  def process_file_chunk(_local_chunk, local_start_byte, remote_chunk)
      when local_start_byte < remote_chunk.start_byte do
    # Invalid state while appending file
    nil
  end

  def process_file_chunk(_local_chunk, local_start_byte, "null") do
    # Invalid state while appending file
    nil
  end

  def process_file_chunk(local_chunk, local_start_byte, remote_chunk)
      when local_start_byte > remote_chunk.start_byte do
    # 1. Create new chunk
    # 2. Get chunkservers for this new chunk
    # 3. Write data to chunk at these chunkservers
  end

  def process_file_chunk(local_chunk, local_start_byte, remote_chunk)
      when local_start_byte == remote_chunk.start_byte do
  end

  @doc """
  Read a file from local_fs in 64 Mb chunks

  ## Examples

      iex> local_path = "/mnt/d/TV Shows/Severance Season 2 Mp4 1080p/Severance S02E01.mp4"
      iex> Gfs.Client.read_file_in_chunks(local_path)

  """
  def for_each_chunk(file_path, task) do
    File.stream!(file_path, @chunk_size)
    |> Enum.reduce(0, fn chunk, start_byte ->
      task.(chunk, start_byte)
      start_byte + byte_size(chunk)
    end)
  end
end
