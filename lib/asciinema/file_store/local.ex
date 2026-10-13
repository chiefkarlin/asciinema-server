defmodule Asciinema.FileStore.Local do
  use Asciinema.Config

  @behaviour Asciinema.FileStore

  @impl true
  def uri(path) do
    abs_path =
      path
      |> full_path()
      |> Path.expand()

    "file://" <> abs_path
  end

  @impl true
  def put_file(dst_path, src_local_path, _content_type) do
    full_dst_path = full_path(dst_path)
    parent_dir = Path.dirname(full_dst_path)

    # Temp file in same directory ensures same filesystem for atomic rename
    tmp_path = "#{full_dst_path}.#{System.unique_integer([:positive])}.tmp"

    with :ok <- File.mkdir_p(parent_dir) do
      try do
        with {:ok, _} <- File.copy(src_local_path, tmp_path) do
          File.rename(tmp_path, full_dst_path)
        end
      after
        # Clean up temp file if it still exists
        File.rm(tmp_path)
      end
    end
  end

  @impl true
  def move_file(from_path, to_path) do
    full_from_path = full_path(from_path)
    full_to_path = full_path(to_path)
    parent_dir = Path.dirname(full_to_path)
    :ok = File.mkdir_p(parent_dir)
    File.rename(full_from_path, full_to_path)
  end

  @impl true
  def delete_file(path) do
    File.rm(full_path(path))
  end

  @impl true
  def direct_upload_supported?, do: false

  @impl true
  def direct_download_supported?, do: false

  @impl true
  def open_stream(path, opts \\ []) do
    chunk_size = Keyword.get(opts, :chunk_size, 65_536)
    abs_path = full_path(path)

    if File.exists?(abs_path) do
      {:ok, File.stream!(abs_path, chunk_size)}
    else
      {:error, :enoent}
    end
  end

  @impl true
  def stat_file(path) do
    case File.stat(full_path(path)) do
      {:ok, %{size: size}} -> {:ok, %{size: size, content_type: nil}}
      {:error, :enoent} -> {:error, :enoent}
      {:error, reason} -> {:error, reason}
    end
  end

  defp full_path(path), do: Path.join(base_path(), path)

  defp base_path, do: config(:path)
end
