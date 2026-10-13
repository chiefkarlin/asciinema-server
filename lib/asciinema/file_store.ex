defmodule Asciinema.FileStore do
  use Asciinema.Config

  @doc "Returns URI for a given path"
  @callback uri(path :: String.t()) :: String.t()

  @doc "Puts file at given path in store"
  @callback put_file(
              dst_path :: String.t(),
              src_local_path :: String.t(),
              content_type :: String.t()
            ) :: :ok | {:error, term}

  @doc "Moves file to a new path"
  @callback move_file(from_path :: Path.t(), to_path :: Path.t()) :: :ok | {:error, term}

  @doc "Deletes file"
  @callback delete_file(path :: String.t()) :: :ok | {:error, term}

  @doc "Returns true if the store supports direct client uploads (signed PUT / resumable)"
  @callback direct_upload_supported?() :: boolean()

  @doc "Returns true if the store supports direct client download redirects (signed GET)"
  @callback direct_download_supported?() :: boolean()

  @doc "Returns a signed direct download URL for a path"
  @callback direct_download_url(path :: String.t(), opts :: keyword()) ::
              {:ok, String.t()} | {:error, term}

  @doc "Initiates a direct upload session or signed PUT URL for a destination path"
  @callback initiate_direct_upload(dst_path :: String.t(), opts :: keyword()) ::
              {:ok, map()} | {:error, term}

  @doc "Opens a lazy stream of binary chunks for a stored object"
  @callback open_stream(path :: String.t(), opts :: keyword()) ::
              {:ok, Enumerable.t()} | {:error, term}

  @doc "Fetches metadata (size, content_type) for a stored object without downloading it"
  @callback stat_file(path :: String.t()) ::
              {:ok, %{size: non_neg_integer(), content_type: String.t() | nil}} | {:error, term}

  @optional_callbacks [
    direct_upload_supported?: 0,
    direct_download_supported?: 0,
    direct_download_url: 2,
    initiate_direct_upload: 2,
    open_stream: 2,
    stat_file: 1
  ]

  # Shortcuts

  def uri(path) do
    adapter().uri(path)
  end

  def put_file(dst_path, src_local_path, content_type) do
    adapter().put_file(dst_path, src_local_path, content_type)
  end

  def move_file(from_path, to_path) do
    adapter().move_file(from_path, to_path)
  end

  def delete_file(path) do
    adapter().delete_file(path)
  end

  def direct_upload_supported? do
    adapter = adapter()
    Code.ensure_loaded?(adapter)

    if function_exported?(adapter, :direct_upload_supported?, 0) do
      adapter.direct_upload_supported?()
    else
      false
    end
  end

  def direct_download_supported? do
    adapter = adapter()
    Code.ensure_loaded?(adapter)

    if function_exported?(adapter, :direct_download_supported?, 0) do
      adapter.direct_download_supported?()
    else
      false
    end
  end

  def direct_download_url(path, opts \\ []) do
    adapter = adapter()
    Code.ensure_loaded?(adapter)

    if function_exported?(adapter, :direct_download_url, 2) do
      adapter.direct_download_url(path, opts)
    else
      {:error, :unsupported}
    end
  end

  def initiate_direct_upload(dst_path, opts \\ []) do
    adapter = adapter()
    Code.ensure_loaded?(adapter)

    if function_exported?(adapter, :initiate_direct_upload, 2) do
      adapter.initiate_direct_upload(dst_path, opts)
    else
      {:error, :unsupported}
    end
  end

  def open_stream(path, opts \\ []) do
    adapter = adapter()
    Code.ensure_loaded?(adapter)

    cond do
      function_exported?(adapter, :open_stream, 2) ->
        adapter.open_stream(path, opts)

      function_exported?(adapter, :open_stream, 1) ->
        adapter.open_stream(path)

      true ->
        fallback_open_stream(path, opts)
    end
  end

  def stream!(path, opts \\ []) do
    case open_stream(path, opts) do
      {:ok, stream} -> stream
      {:error, reason} -> raise "failed to open stream for #{path}: #{inspect(reason)}"
    end
  end

  def stat_file(path) do
    adapter = adapter()
    Code.ensure_loaded?(adapter)

    if function_exported?(adapter, :stat_file, 1) do
      adapter.stat_file(path)
    else
      fallback_stat_file(path)
    end
  end

  def head_object(path), do: stat_file(path)

  def adapter do
    Keyword.fetch!(config(), :adapter)
  end

  defp fallback_open_stream(path, opts) do
    chunk_size = Keyword.get(opts, :chunk_size, 65_536)

    case uri(path) do
      "file://" <> abs_path ->
        if File.exists?(abs_path) do
          {:ok, File.stream!(abs_path, chunk_size)}
        else
          {:error, :enoent}
        end

      "http" <> _ = url ->
        Asciinema.FileStore.GCS.stream_http_url(url, opts)
    end
  end

  defp fallback_stat_file(path) do
    case uri(path) do
      "file://" <> abs_path ->
        case File.stat(abs_path) do
          {:ok, %{size: size}} -> {:ok, %{size: size, content_type: nil}}
          {:error, :enoent} -> {:error, :enoent}
          {:error, reason} -> {:error, reason}
        end

      _ ->
        {:error, :unsupported}
    end
  end
end
