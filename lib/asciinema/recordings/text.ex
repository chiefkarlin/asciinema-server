defmodule Asciinema.Recordings.Text do
  alias Asciinema.Recordings
  alias Asciinema.Recordings.Asciicast
  alias Asciinema.Vt

  @default_scrollback_limit 10_000
  @max_text_feed_bytes 16 * 1024 * 1024

  def default_scrollback_limit, do: @default_scrollback_limit

  def text(%Asciicast{term_cols: cols, term_rows: rows} = asciicast, opts \\ [])
      when is_list(opts) do
    asciicast
    |> Recordings.event_stream()
    |> text_from_events(cols, rows, opts)
  end

  def text_from_events(events, cols, rows, opts \\ []) when is_list(opts) do
    scrollback_limit = Keyword.get(opts, :scrollback_limit, @default_scrollback_limit)
    max_feed_bytes = Keyword.get(opts, :max_feed_bytes, @max_text_feed_bytes)
    {:ok, vt} = Vt.new(cols, rows, scrollback_limit)

    Enum.reduce_while(events, 0, fn {_, code, data}, bytes_fed ->
      case code do
        "o" ->
          Vt.feed(vt, data)
          next_bytes = bytes_fed + byte_size(data)

          if next_bytes >= max_feed_bytes do
            {:halt, next_bytes}
          else
            {:cont, next_bytes}
          end

        "r" ->
          {cols, rows} = data
          Vt.resize(vt, cols, rows)
          {:cont, bytes_fed}

        _ ->
          {:cont, bytes_fed}
      end
    end)

    Vt.text(vt)
  end
end
