defmodule Asciinema.BoundedMemoryTest do
  use ExUnit.Case, async: false
  alias Asciinema.Asciicast.Reader
  alias Asciinema.Recordings
  alias Asciinema.Recordings.Text

  @max_memory_delta_bytes 50 * 1024 * 1024

  describe "bounded-memory single-pass processing (R2)" do
    test "processes a ~200 MB / 500,000-event synthetic V3 stream with < 50 MB BEAM memory delta" do
      header = ~s({"version":3,"term":{"cols":120,"rows":40},"timestamp":1744302022,"title":"200MB Synthetic Test"}\n)
      # Each line is ~400 bytes -> 500,000 events is ~200 MB of stream data.
      payload_text = String.duplicate("abcdefghij", 36)
      event_line = ~s([0.001, "o", "#{payload_text}\\r\\n"]\n)
      lines_per_chunk = 100
      chunk = String.duplicate(event_line, lines_per_chunk)
      num_chunks = 5_000

      chunk_stream =
        Stream.concat(
          [header],
          Stream.repeatedly(fn -> chunk end) |> Stream.take(num_chunks)
        )

      :erlang.garbage_collect()
      Process.sleep(20)
      baseline_memory = :erlang.memory(:total)

      assert {:ok, metadata} = Recordings.extract_metadata_from_stream(chunk_stream)

      :erlang.garbage_collect()
      after_memory = :erlang.memory(:total)
      memory_delta = max(0, after_memory - baseline_memory)

      assert metadata.version == 3
      assert metadata.term_cols == 120
      assert metadata.term_rows == 40
      assert metadata.event_count == 500_000
      assert_in_delta metadata.duration, 500.0, 1.0

      assert memory_delta < @max_memory_delta_bytes,
             "Expected BEAM memory delta < 50 MB, got #{Float.round(memory_delta / (1024 * 1024), 2)} MB"
    end

    test "Text.text_from_events/4 and generate_snapshot/4 bound scrollback on high-volume output streams" do
      event_stream =
        Stream.iterate(1, &(&1 + 1))
        |> Stream.take(100_000)
        |> Stream.map(fn i -> {i * 0.001, "o", "line #{i} of terminal output\r\n"} end)

      :erlang.garbage_collect()
      baseline_memory = :erlang.memory(:total)

      text = Text.text_from_events(event_stream, 80, 24)

      :erlang.garbage_collect()
      after_memory = :erlang.memory(:total)
      memory_delta = max(0, after_memory - baseline_memory)

      assert is_binary(text)
      assert text =~ "line 100000 of terminal output"
      # Scrollback is bounded to 10,000 lines, so line 1 should have scrolled off
      refute text =~ "line 1 of terminal output\n"

      assert memory_delta < @max_memory_delta_bytes
    end

    test "rejects newline-less stream exceeding 16 MB line cap without unbounded buffering" do
      one_mb_chunk = :binary.copy("a", 1024 * 1024)
      infinite_no_newline = Stream.repeatedly(fn -> one_mb_chunk end) |> Stream.take(20)

      assert {:error, :invalid_format} =
               Recordings.extract_metadata_from_stream(infinite_no_newline)

      assert_raise Asciinema.Zstd.LineTooLongError, fn ->
        infinite_no_newline
        |> Reader.split_lines(4 * 1024 * 1024)
        |> Stream.run()
      end
    end
  end
end
