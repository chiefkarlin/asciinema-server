defmodule Asciinema.Asciicast.Reader do
  alias Asciinema.Zstd

  @chunk_size 64 * 1024
  @max_line_bytes 16 * 1024 * 1024

  def max_line_bytes, do: @max_line_bytes

  @spec read_iodata!(Path.t() | {:stream, Enumerable.t()}, keyword()) :: iodata()
  def read_iodata!(source, opts \\ [])

  def read_iodata!(path, opts) when is_binary(path) and is_list(opts) do
    max_bytes = Keyword.get(opts, :max_bytes)

    if compressed?(path, opts) do
      path
      |> Zstd.stream!(@chunk_size)
      |> take_iodata_bounded!(max_bytes)
    else
      if max_bytes != nil and File.stat!(path).size > max_bytes do
        raise ArgumentError, "file exceeds maximum allowed size of #{max_bytes} bytes"
      end

      [File.read!(path)]
    end
  end

  def read_iodata!({:stream, chunk_enum}, opts) when is_list(opts) do
    max_bytes = Keyword.get(opts, :max_bytes)

    stream =
      if Keyword.get(opts, :compressed, false) do
        Zstd.decompress_stream(chunk_enum)
      else
        chunk_enum
      end

    take_iodata_bounded!(stream, max_bytes)
  end

  defp take_iodata_bounded!(stream, nil), do: Enum.to_list(stream)

  defp take_iodata_bounded!(stream, max_bytes) when is_integer(max_bytes) and max_bytes > 0 do
    {chunks, _total} =
      Enum.reduce(stream, {[], 0}, fn chunk, {acc, total} ->
        new_total = total + byte_size(chunk)

        if new_total > max_bytes do
          raise ArgumentError,
                "decompressed stream exceeds maximum allowed size of #{max_bytes} bytes"
        end

        {[chunk | acc], new_total}
      end)

    Enum.reverse(chunks)
  end

  @spec stream_lines!(Path.t() | {:stream, Enumerable.t()} | Enumerable.t(), keyword()) ::
          Enumerable.t()
  def stream_lines!(source, opts \\ [])

  def stream_lines!(path, opts) when is_binary(path) and is_list(opts) do
    max_line_bytes = Keyword.get(opts, :max_line_bytes, @max_line_bytes)

    if compressed?(path, opts) do
      if max_line_bytes == @max_line_bytes do
        Zstd.stream!(path, :line)
      else
        path
        |> File.stream!(@chunk_size)
        |> Zstd.decompress_stream_lines(max_line_bytes: max_line_bytes)
      end
    else
      path
      |> File.stream!(@chunk_size)
      |> split_lines(max_line_bytes: max_line_bytes)
    end
  end

  def stream_lines!({:stream, chunk_enum}, opts) when is_list(opts) do
    stream_lines_from_chunks!(chunk_enum, opts)
  end

  def stream_lines!(chunk_enum, opts) when is_list(opts) do
    stream_lines_from_chunks!(chunk_enum, opts)
  end

  defp stream_lines_from_chunks!(chunk_enum, opts) do
    max_line_bytes = Keyword.get(opts, :max_line_bytes, @max_line_bytes)

    case Keyword.fetch(opts, :compressed) do
      {:ok, true} ->
        Zstd.decompress_stream_lines(chunk_enum, max_line_bytes: max_line_bytes)

      {:ok, false} ->
        split_lines(chunk_enum, max_line_bytes: max_line_bytes)

      :error ->
        auto_stream_lines_from_chunks(chunk_enum, max_line_bytes)
    end
  end

  defp auto_stream_lines_from_chunks(chunk_enum, max_line_bytes) do
    Stream.concat(chunk_enum, [:eof])
    |> Stream.transform(
      fn -> %{mode: :detect, prefix: <<>>, zstd_ctx: nil, buffer: <<>>, max_line_bytes: max_line_bytes} end,
      fn
        :eof, %{mode: :detect, prefix: prefix, max_line_bytes: max} = state ->
          if zstd_magic?(prefix) do
            {:ok, ctx} = :zstd.context(:decompress)
            decompressed = Zstd.decompress_chunk!(ctx, prefix) <> Zstd.finish_decompress!(ctx)
            :zstd.close(ctx)

            case Zstd.extract_lines(decompressed, true, max) do
              {:emit, lines, _rest} -> {lines, %{state | prefix: <<>>}}
              :halt -> {[], %{state | prefix: <<>>}}
            end
          else
            case Zstd.extract_lines(prefix, true, max) do
              {:emit, lines, _rest} -> {lines, %{state | prefix: <<>>}}
              :halt -> {[], %{state | prefix: <<>>}}
            end
          end

        :eof, %{mode: :zstd, zstd_ctx: ctx, buffer: buffer, max_line_bytes: max} = state ->
          decompressed = Zstd.finish_decompress!(ctx)
          buffer = buffer <> decompressed

          case Zstd.extract_lines(buffer, true, max) do
            {:emit, lines, rest} -> {lines, %{state | buffer: rest}}
            :halt -> {[], %{state | buffer: <<>>}}
          end

        :eof, %{mode: :plain, buffer: buffer, max_line_bytes: max} = state ->
          case Zstd.extract_lines(buffer, true, max) do
            {:emit, lines, rest} -> {lines, %{state | buffer: rest}}
            :halt -> {[], %{state | buffer: <<>>}}
          end

        chunk, %{mode: :detect, prefix: prefix, max_line_bytes: max} = state when is_binary(chunk) ->
          combined = prefix <> chunk

          if byte_size(combined) < 4 do
            {[], %{state | prefix: combined}}
          else
            if zstd_magic?(combined) do
              {:ok, ctx} = :zstd.context(:decompress)
              decompressed = Zstd.decompress_chunk!(ctx, combined)

              case Zstd.extract_lines(decompressed, false, max) do
                {:emit, lines, rest} ->
                  {lines, %{state | mode: :zstd, prefix: <<>>, zstd_ctx: ctx, buffer: rest}}

                :need_more ->
                  {[], %{state | mode: :zstd, prefix: <<>>, zstd_ctx: ctx, buffer: decompressed}}
              end
            else
              case Zstd.extract_lines(combined, false, max) do
                {:emit, lines, rest} ->
                  {lines, %{state | mode: :plain, prefix: <<>>, buffer: rest}}

                :need_more ->
                  {[], %{state | mode: :plain, prefix: <<>>, buffer: combined}}
              end
            end
          end

        chunk, %{mode: :zstd, zstd_ctx: ctx, buffer: buffer, max_line_bytes: max} = state
        when is_binary(chunk) ->
          decompressed = Zstd.decompress_chunk!(ctx, chunk)
          buffer = buffer <> decompressed

          case Zstd.extract_lines(buffer, false, max) do
            {:emit, lines, rest} -> {lines, %{state | buffer: rest}}
            :need_more -> {[], %{state | buffer: buffer}}
          end

        chunk, %{mode: :plain, buffer: buffer, max_line_bytes: max} = state
        when is_binary(chunk) ->
          buffer = buffer <> chunk

          case Zstd.extract_lines(buffer, false, max) do
            {:emit, lines, rest} -> {lines, %{state | buffer: rest}}
            :need_more -> {[], %{state | buffer: buffer}}
          end
      end,
      fn
        %{zstd_ctx: ctx} when ctx != nil -> :zstd.close(ctx)
        _ -> :ok
      end
    )
  end

  defp zstd_magic?(<<0x28, 0xB5, 0x2F, 0xFD, _::binary>>), do: true
  defp zstd_magic?(_), do: false

  @spec split_lines(Enumerable.t(), keyword() | pos_integer()) :: Enumerable.t()
  def split_lines(chunk_enum, opts \\ [])

  def split_lines(chunk_enum, max_line_bytes)
      when is_integer(max_line_bytes) and max_line_bytes > 0 do
    split_lines(chunk_enum, max_line_bytes: max_line_bytes)
  end

  def split_lines(chunk_enum, opts) when is_list(opts) do
    max_line_bytes = Keyword.get(opts, :max_line_bytes, @max_line_bytes)

    Stream.concat(chunk_enum, [:eof])
    |> Stream.transform(
      fn -> %{buffer: <<>>, max_line_bytes: max_line_bytes} end,
      fn
        :eof, state ->
          case Zstd.extract_lines(state.buffer, true, state.max_line_bytes) do
            {:emit, lines, rest} -> {lines, %{state | buffer: rest}}
            :halt -> {[], %{state | buffer: <<>>}}
          end

        chunk, state when is_binary(chunk) ->
          buffer = state.buffer <> chunk

          case Zstd.extract_lines(buffer, false, state.max_line_bytes) do
            {:emit, lines, rest} -> {lines, %{state | buffer: rest}}
            :need_more -> {[], %{state | buffer: buffer}}
          end
      end,
      fn _state -> :ok end
    )
  end

  @spec compressed?(Path.t(), keyword()) :: boolean()
  def compressed?(path, opts \\ []) when is_binary(path) and is_list(opts) do
    Keyword.get_lazy(opts, :compressed, fn -> compressed_file?(path) end)
  end

  @spec compressed_magic?(binary()) :: boolean()
  def compressed_magic?(<<0x28, 0xB5, 0x2F, 0xFD, _::binary>>), do: true
  def compressed_magic?(<<0x1F, 0x8B, _::binary-size(2), _::binary>>), do: true
  def compressed_magic?(_), do: false

  defp compressed_file?(path) do
    case File.open(path, [:read, :binary], fn file -> IO.binread(file, 4) end) do
      {:ok, bytes} when is_binary(bytes) -> compressed_magic?(bytes)
      {:ok, :eof} -> false
      {:error, reason} -> raise File.Error, reason: reason, action: "open file", path: path
    end
  end
end
