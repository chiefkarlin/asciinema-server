defmodule Asciinema.Asciicast.V3 do
  alias Asciinema.Asciicast.{EventStream, Reader}
  alias Asciinema.{Colors, Quantizer, Zstd}

  defmodule Writer do
    @enforce_keys [:file, :prev_time, :time_quantizer]
    defstruct [:file, :prev_time, :time_quantizer]
  end

  def event_stream(source, opts \\ []) when is_list(opts) do
    source
    |> Reader.stream_lines!(opts)
    |> Stream.concat([:eof])
    |> Stream.transform(:header, fn
      :eof, :header ->
        raise MatchError, term: []

      :eof, {:events, _idle_time_limit} = state ->
        {[], state}

      line, :header ->
        "{" <> _ = line
        header = Jason.decode!(line)
        3 = header["version"]
        {[], {:events, header["idle_time_limit"]}}

      "\n", {:events, _} = state ->
        {[], state}

      "#" <> _, {:events, _} = state ->
        {[], state}

      line, {:events, idle_time_limit} = state ->
        events =
          line
          |> Jason.decode!()
          |> parse_event()
          |> Enum.map(&EventStream.cap_relative_time(&1, idle_time_limit))

        {events, state}
    end)
    |> EventStream.to_absolute_time()
  end

  defp parse_event([time, "r", data]) when is_number(time) and time >= 0 and is_binary(data) do
    with [cols, rows] <- String.split(data, "x"),
         {cols, ""} when cols > 0 <- Integer.parse(cols),
         {rows, ""} when rows > 0 <- Integer.parse(rows) do
      [{time, "r", {cols, rows}}]
    else
      _ -> []
    end
  end

  defp parse_event([time, code, data])
       when is_number(time) and time >= 0 and is_binary(code) and is_binary(data) do
    [{time, code, data}]
  end

  def fetch_metadata(source, opts \\ []) when is_list(opts) do
    source
    |> Reader.stream_lines!(opts)
    |> fetch_metadata_from_line_stream()
  rescue
    Zstd.LineTooLongError ->
      {:error, :invalid_format}

    RuntimeError ->
      {:error, :invalid_format}
  end

  def fetch_metadata_from_line_stream(lines) do
    result =
      Enum.reduce_while(lines, :header, fn
        line, :header ->
          case parse_header_line(line) do
            {:ok, header} ->
              {:cont, {:events, header, header["idle_time_limit"], 0, 0}}

            {:error, _} = err ->
              {:halt, err}
          end

        line, {:events, header, idle_time_limit, abs_time, count} ->
          case reduce_event_line(line, idle_time_limit, abs_time, count) do
            {:ok, next_abs_time, next_count} ->
              {:cont, {:events, header, idle_time_limit, next_abs_time, next_count}}

            {:error, _} = err ->
              {:halt, err}
          end
      end)

    case result do
      :header ->
        {:error, :invalid_format}

      {:events, header, _idle_time_limit, abs_time, event_count} ->
        {:ok, build_metadata(header, %{duration: abs_time / 1, event_count: event_count})}

      {:error, _} = err ->
        err
    end
  rescue
    Zstd.LineTooLongError ->
      {:error, :invalid_format}

    RuntimeError ->
      {:error, :invalid_format}
  end

  def build_metadata(header, stats) do
    %{
      version: 3,
      term_cols: get_in(header, ["term", "cols"]),
      term_rows: get_in(header, ["term", "rows"]),
      term_type: get_in(header, ["term", "type"]),
      term_version: get_in(header, ["term", "version"]),
      term_theme_fg: get_in(header, ["term", "theme", "fg"]),
      term_theme_bg: get_in(header, ["term", "theme", "bg"]),
      term_theme_palette: get_in(header, ["term", "theme", "palette"]),
      command: header["command"],
      duration: stats.duration,
      event_count: stats.event_count,
      recorded_at: from_unix(header["timestamp"]),
      title: header["title"],
      env: header["env"] || %{},
      idle_time_limit: header["idle_time_limit"],
      shell: get_in(header, ["env", "SHELL"])
    }
  end

  def parse_header_line(line) when is_binary(line) do
    with {:ok, %{"version" => 3} = header} <- Jason.decode(line),
         :ok <- validate_theme(get_in(header, ["term", "theme"])) do
      {:ok, header}
    else
      {:ok, %{"version" => version}} ->
        {:error, {:invalid_version, version}}

      {:ok, _} ->
        {:error, :invalid_format}

      {:error, %Jason.DecodeError{}} ->
        {:error, :invalid_format}

      {:error, :invalid_theme} ->
        {:error, :invalid_format}
    end
  end

  def reduce_event_line("\n", _idle_time_limit, abs_time, count) do
    {:ok, abs_time, count}
  end

  def reduce_event_line("#" <> _, _idle_time_limit, abs_time, count) do
    {:ok, abs_time, count}
  end

  def reduce_event_line(line, idle_time_limit, abs_time, count) do
    case decode_and_parse_event(line) do
      {:ok, [{rel_time, _code, _data}]} ->
        capped_rel = if idle_time_limit, do: min(rel_time, idle_time_limit), else: rel_time
        next_abs = if count == 0, do: capped_rel, else: abs_time + capped_rel
        {:ok, next_abs, count + 1}

      {:ok, []} ->
        {:ok, abs_time, count}

      {:error, :invalid_format} ->
        {:error, :invalid_format}
    end
  end

  defp decode_and_parse_event(line) do
    case Jason.decode(line) do
      {:ok, raw} ->
        try do
          {:ok, parse_event(raw)}
        rescue
          FunctionClauseError -> {:error, :invalid_format}
        end

      {:error, %Jason.DecodeError{}} ->
        {:error, :invalid_format}
    end
  end

  defp validate_theme(nil), do: :ok

  defp validate_theme(%{"fg" => _, "bg" => _, "palette" => _}), do: :ok

  defp validate_theme(_theme), do: {:error, :invalid_theme}

  def create(path, {cols, rows}, fields \\ []) do
    file = File.open!(path, [:write, :utf8])
    timestamp = Keyword.get(fields, :timestamp)
    title = Keyword.get(fields, :title)
    env = drop_empty(Keyword.get(fields, :env) || %{})

    term =
      [
        cols: cols,
        rows: rows,
        type: Keyword.get(fields, :term_type),
        version: Keyword.get(fields, :term_version),
        theme: format_theme(Keyword.get(fields, :term_theme))
      ]
      |> drop_empty()
      |> Jason.OrderedObject.new()

    header =
      [version: 3, term: term, timestamp: timestamp, title: title, env: env]
      |> drop_empty()
      |> Jason.OrderedObject.new()

    with :ok <- IO.write(file, Jason.encode!(header) <> "\n") do
      {:ok, %Writer{file: file, prev_time: 0, time_quantizer: Quantizer.new(1_000)}}
    end
  end

  def write_event(
        %Writer{} = writer,
        time,
        type,
        data
      )
      when type in ["o", "i", "m", "x"] do
    {time_quantizer, dt} = Quantizer.next(writer.time_quantizer, time - writer.prev_time)
    data = Jason.encode!(data)
    event = "[#{format_time(dt)}, \"#{type}\", #{data}]"

    with :ok <- IO.write(writer.file, event <> "\n") do
      {:ok, %{writer | prev_time: time, time_quantizer: time_quantizer}}
    end
  end

  def write_event(
        %Writer{} = writer,
        time,
        "r",
        {cols, rows}
      ) do
    {time_quantizer, dt} = Quantizer.next(writer.time_quantizer, time - writer.prev_time)
    data = Jason.encode!("#{cols}x#{rows}")
    event = "[#{format_time(dt)}, \"r\", #{data}]"

    with :ok <- IO.write(writer.file, event <> "\n") do
      {:ok, %{writer | prev_time: time, time_quantizer: time_quantizer}}
    end
  end

  defp format_theme(nil), do: nil

  defp format_theme(theme) do
    palette = Enum.map_join(theme.palette, ":", &Colors.hex/1)

    Jason.OrderedObject.new(
      fg: Colors.hex(theme.fg),
      bg: Colors.hex(theme.bg),
      palette: palette
    )
  end

  defp format_time(time) do
    whole = div(time, 1_000_000)

    decimal =
      time
      |> rem(1_000_000)
      |> to_string()
      |> String.pad_leading(6, "0")
      |> String.slice(0, 3)

    decimal =
      case decimal do
        "" -> "0"
        d -> d
      end

    "#{whole}.#{decimal}"
  end

  defp drop_empty(map) when is_map(map) do
    map
    |> Enum.filter(fn {_k, v} -> v != nil and v != "" and v != %{} end)
    |> Enum.into(%{})
  end

  defp drop_empty(kv) when is_list(kv) do
    Enum.filter(kv, fn {_k, v} -> v != nil and v != "" and v != %{} end)
  end

  def close(%Writer{file: file}), do: File.close(file)

  defp from_unix(ts) when is_integer(ts) do
    case DateTime.from_unix(ts) do
      {:ok, time} -> time
      {:error, _} -> nil
    end
  end

  defp from_unix(_), do: nil
end
