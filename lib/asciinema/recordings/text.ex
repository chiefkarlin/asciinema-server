defmodule Asciinema.Recordings.Text do
  alias Asciinema.Recordings
  alias Asciinema.Recordings.Asciicast
  alias Asciinema.Vt

  @default_scrollback_limit 10_000

  def default_scrollback_limit, do: @default_scrollback_limit

  def text(%Asciicast{term_cols: cols, term_rows: rows} = asciicast, opts \\ [])
      when is_list(opts) do
    asciicast
    |> Recordings.event_stream()
    |> text_from_events(cols, rows, opts)
  end

  def text_from_events(events, cols, rows, opts \\ []) when is_list(opts) do
    scrollback_limit = Keyword.get(opts, :scrollback_limit, @default_scrollback_limit)
    {:ok, vt} = Vt.new(cols, rows, scrollback_limit)

    Enum.each(events, fn {_, code, data} ->
      case code do
        "o" ->
          Vt.feed(vt, data)

        "r" ->
          {cols, rows} = data
          Vt.resize(vt, cols, rows)

        _ ->
          :ok
      end
    end)

    Vt.text(vt)
  end
end
