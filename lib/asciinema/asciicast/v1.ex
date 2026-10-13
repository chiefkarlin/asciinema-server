defmodule Asciinema.Asciicast.V1 do
  alias Asciinema.Asciicast.{EventStream, Reader}

  @max_v1_size 32 * 1024 * 1024

  def max_v1_size, do: @max_v1_size

  def event_stream(source, opts \\ []) when is_list(opts) do
    opts = Keyword.put_new(opts, :max_bytes, @max_v1_size)

    asciicast =
      source
      |> Reader.read_iodata!(opts)
      |> Jason.decode!()

    1 = asciicast["version"]

    asciicast
    |> Map.fetch!("stdout")
    |> Stream.map(&parse_event/1)
    |> EventStream.to_absolute_time()
  end

  defp parse_event([time, data]) when is_number(time) and time >= 0 and is_binary(data) do
    {time, "o", data}
  end

  def fetch_metadata(source, opts \\ []) when is_list(opts) do
    opts = Keyword.put_new(opts, :max_bytes, @max_v1_size)

    with {:ok, attrs} <- parse_file(source, opts),
         {:ok, stats} <- get_event_stats_from_attrs(attrs) do
      {:ok, build_metadata(attrs, stats)}
    end
  end

  def fetch_metadata_from_json(json) when is_binary(json) or is_list(json) do
    with {:ok, attrs} <- decode_v1_attrs(json),
         {:ok, stats} <- get_event_stats_from_attrs(attrs) do
      {:ok, build_metadata(attrs, stats)}
    end
  end

  defp build_metadata(attrs, stats) do
    %{
      version: 1,
      term_cols: attrs["width"],
      term_rows: attrs["height"],
      term_type: get_in(attrs, ["env", "TERM"]),
      command: attrs["command"],
      duration: stats.duration,
      event_count: stats.event_count,
      title: attrs["title"],
      shell: get_in(attrs, ["env", "SHELL"]),
      env: attrs["env"] || %{}
    }
  end

  defp parse_file(path, opts) do
    json = Reader.read_iodata!(path, opts)
    decode_v1_attrs(json)
  rescue
    ArgumentError ->
      {:error, :invalid_format}
  end

  defp decode_v1_attrs(json) do
    case Jason.decode(json) do
      {:ok, %{"version" => 1} = attrs} ->
        {:ok, attrs}

      {:ok, %{"version" => version}} ->
        {:error, {:invalid_version, version}}

      {:ok, _} ->
        {:error, :invalid_format}

      {:error, %Jason.DecodeError{}} ->
        {:error, :invalid_format}
    end
  end

  defp get_event_stats_from_attrs(%{"stdout" => stdout}) when is_list(stdout) do
    stats =
      stdout
      |> Stream.map(&parse_event/1)
      |> EventStream.to_absolute_time()
      |> EventStream.stats()

    {:ok, stats}
  rescue
    FunctionClauseError ->
      {:error, :invalid_format}
  end

  defp get_event_stats_from_attrs(_), do: {:error, :invalid_format}
end
