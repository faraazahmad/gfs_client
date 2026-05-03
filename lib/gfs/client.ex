defmodule Gfs.Client do
  @moduledoc """
  Documentation for `Gfs.Client`.

  Implements the GFS-style lease-based append protocol on top of the manager
  and chunkserver HTTP APIs.
  """

  # 64 MiB
  @chunk_size 64 * 1024 * 1024

  @manager_base_url "http://localhost:4000"
  @max_attempts 5

  @doc """
  Append `content` (a binary of any size) to the file at `gfs_path`.

  Flow:
    1. URL-encode the gfs_path with Base.encode16/1.
    2. Request a lease from the manager.
    3. On `file_not_found` -> create the file then retry the lease.
    4. On `lease_conflict` -> backoff (100 + rand 200 ms) and retry.
    5. With a lease, PUT base64-encoded content to the primary chunkserver's
       `/append/chunk/<chunk_uniq_id>` endpoint.
    6. On `chunk_full` -> ask the manager to allocate a new chunk and retry.
    7. On `lease_expired` -> retry the lease.
    8. On other transient errors -> retry with backoff.
    9. Cap retries at `@max_attempts`; return `{:error, :max_retries}` on exceed.

  Returns `:ok` on success.
  """
  def append_file(gfs_path, content) do
    encoded_path = Base.encode16(gfs_path)
    do_append(encoded_path, content, 1)
  end

  defp do_append(_encoded_path, _content, attempt) when attempt > @max_attempts do
    IO.puts("append_file: exceeded max attempts (#{@max_attempts})")
    {:error, :max_retries}
  end

  defp do_append(encoded_path, content, attempt) do
    IO.puts("append_file: attempt #{attempt}")

    case get_lease(encoded_path) do
      {:ok, lease} ->
        case append_via_primary(lease, content) do
          :ok ->
            :ok

          {:error, :chunk_full} ->
            IO.puts("append_file: chunk_full, allocating new chunk")
            _ = create_next_chunk(encoded_path)
            backoff()
            do_append(encoded_path, content, attempt + 1)

          {:error, :lease_expired} ->
            IO.puts("append_file: lease_expired, retrying")
            do_append(encoded_path, content, attempt + 1)

          {:error, :transient} ->
            IO.puts("append_file: transient primary error, retrying")
            backoff()
            do_append(encoded_path, content, attempt + 1)
        end

      {:error, :file_not_found} ->
        IO.puts("append_file: file not found, creating")
        _ = ensure_file_exists(encoded_path)
        do_append(encoded_path, content, attempt + 1)

      {:error, :lease_conflict} ->
        IO.puts("append_file: lease conflict, backing off")
        backoff()
        do_append(encoded_path, content, attempt + 1)

      {:error, :under_replicated} ->
        IO.puts("append_file: under_replicated, backing off")
        backoff()
        do_append(encoded_path, content, attempt + 1)

      {:error, :transient} ->
        IO.puts("append_file: transient lease error, backing off")
        backoff()
        do_append(encoded_path, content, attempt + 1)
    end
  end

  defp backoff do
    Process.sleep(100 + :rand.uniform(200))
  end

  # Acquire a lease from the manager for the next writable chunk of the file.
  defp get_lease(encoded_path) do
    url = "#{@manager_base_url}/file/#{encoded_path}/lease"

    case HTTPoison.get(url, [{"Content-Type", "application/json"}]) do
      {:ok, %HTTPoison.Response{status_code: 200, body: body}} ->
        case Poison.decode(body) do
          {:ok, decoded} -> {:ok, decoded}
          {:error, _} -> {:error, :transient}
        end

      {:ok, %HTTPoison.Response{status_code: 404}} ->
        {:error, :file_not_found}

      {:ok, %HTTPoison.Response{status_code: 409}} ->
        {:error, :lease_conflict}

      {:ok, %HTTPoison.Response{status_code: 503}} ->
        {:error, :under_replicated}

      {:ok, %HTTPoison.Response{status_code: status}} ->
        IO.puts("get_lease: unexpected status #{status}")
        {:error, :transient}

      {:error, %HTTPoison.Error{reason: reason}} ->
        IO.puts("get_lease: network error #{inspect(reason)}")
        {:error, :transient}
    end
  end

  # PUT base64-encoded content to the primary chunkserver for this lease.
  defp append_via_primary(lease, content) do
    primary = Map.fetch!(lease, "primary")
    secondaries = Map.get(lease, "secondaries", [])
    chunk = Map.fetch!(lease, "chunk")
    chunk_uniq_id = Map.fetch!(chunk, "uniq_id")
    primary_port = Map.fetch!(primary, "http_port")
    primary_host = Map.get(primary, "host", "localhost")
    primary_id = Map.fetch!(primary, "id")
    lease_id = Map.fetch!(lease, "lease_id")

    secondaries_payload =
      Enum.map(secondaries, fn s ->
        %{
          "http_port" => Map.fetch!(s, "http_port"),
          "host" => Map.get(s, "host", "localhost")
        }
      end)

    body =
      Poison.encode!(%{
        "lease_id" => lease_id,
        "primary_chunk_server_id" => primary_id,
        "secondaries" => secondaries_payload,
        "content" => Base.encode64(content)
      })

    url = "http://#{primary_host}:#{primary_port}/append/chunk/#{chunk_uniq_id}"

    case HTTPoison.put(url, body, [{"Content-Type", "application/json"}],
           recv_timeout: 30_000
         ) do
      {:ok, %HTTPoison.Response{status_code: 200, body: resp_body}} ->
        IO.puts("append_via_primary: ok #{resp_body}")
        :ok

      {:ok, %HTTPoison.Response{status_code: 422}} ->
        {:error, :chunk_full}

      {:ok, %HTTPoison.Response{status_code: 409}} ->
        {:error, :lease_expired}

      {:ok, %HTTPoison.Response{status_code: status}} when status in [500, 502, 503] ->
        IO.puts("append_via_primary: transient status #{status}")
        {:error, :transient}

      {:ok, %HTTPoison.Response{status_code: status, body: resp_body}} ->
        IO.puts("append_via_primary: unexpected status #{status} body #{resp_body}")
        {:error, :transient}

      {:error, %HTTPoison.Error{reason: reason}} ->
        IO.puts("append_via_primary: network error #{inspect(reason)}")
        {:error, :transient}
    end
  end

  # Create the file (and its initial chunk replicas) on the manager.
  defp ensure_file_exists(encoded_path) do
    url = "#{@manager_base_url}/file/#{encoded_path}"

    case HTTPoison.post(url, "", [{"Content-Type", "application/json"}]) do
      {:ok, %HTTPoison.Response{status_code: 200}} ->
        :ok

      {:ok, %HTTPoison.Response{status_code: status, body: body}} ->
        IO.puts("ensure_file_exists: status #{status} body #{body}")
        {:error, :transient}

      {:error, %HTTPoison.Error{reason: reason}} ->
        IO.puts("ensure_file_exists: network error #{inspect(reason)}")
        {:error, :transient}
    end
  end

  # Ask the manager to allocate the next logical chunk for an existing file.
  defp create_next_chunk(encoded_path) do
    url = "#{@manager_base_url}/file/#{encoded_path}/chunk"

    case HTTPoison.post(url, "", [{"Content-Type", "application/json"}]) do
      {:ok, %HTTPoison.Response{status_code: 200}} ->
        :ok

      {:ok, %HTTPoison.Response{status_code: status, body: body}} ->
        IO.puts("create_next_chunk: status #{status} body #{body}")
        {:error, :transient}

      {:error, %HTTPoison.Error{reason: reason}} ->
        IO.puts("create_next_chunk: network error #{inspect(reason)}")
        {:error, :transient}
    end
  end

  @doc """
  Read a file from local FS, divide it into @chunk_size pieces, and append each piece
  to the GFS file at `gfs_path` via the lease-based append protocol.

  Returns `:ok` on success, or `{:error, reason}` on the first failed append.

  ## Examples
      iex> local_path = "/mnt/d/TV Shows/Severance Season 2 Mp4 1080p/Severance S02E01.mp4"
      iex> Gfs.Client.write_file_from_local_fs(local_path, "severance/s02/ep01.mp4")
  """
  def write_file_from_local_fs(local_path, gfs_path) do
    write_file_from_local_fs(local_path, gfs_path, false)
  end

  def write_file_from_local_fs("", _gfs_path, _force) do
    raise "Invalid local path"
  end

  def write_file_from_local_fs(_local_path, "", _force) do
    raise "Invalid gfs path"
  end

  # When force is false (preferred method) file is appended only.
  def write_file_from_local_fs(local_path, gfs_path, false) do
    expanded = Path.expand(local_path)

    unless File.exists?(expanded) do
      raise "Local file not found: #{expanded}"
    end

    IO.puts("write_file_from_local_fs: streaming #{expanded} -> gfs:#{gfs_path}")

    File.stream!(expanded, @chunk_size)
    |> Enum.reduce_while({:ok, 0}, fn bytes, {:ok, offset} ->
      size = byte_size(bytes)
      IO.puts("write_file_from_local_fs: appending #{size} bytes at offset #{offset}")

      case append_file(gfs_path, bytes) do
        :ok ->
          {:cont, {:ok, offset + size}}

        {:error, reason} = err ->
          IO.puts("write_file_from_local_fs: append failed at offset #{offset}: #{inspect(reason)}")
          {:halt, err}
      end
    end)
    |> case do
      {:ok, total} ->
        IO.puts("write_file_from_local_fs: done, #{total} bytes uploaded")
        :ok

      {:error, _} = err ->
        err
    end
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

  def process_file_chunk(_local_chunk, _local_start_byte, "null") do
    # Invalid state while appending file, first chunk is created on file creation
    nil
  end

  def process_file_chunk(local_chunk_bytes, local_chunk_start_byte, remote_chunk_metadata)
      when local_chunk_start_byte > remote_chunk_metadata.start_byte do
    IO.puts(local_chunk_bytes)
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
