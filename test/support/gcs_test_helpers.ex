defmodule Asciinema.GcsTestHelpers do
  import ExUnit.Callbacks, only: [start_supervised!: 1]

  def start_mock_gcs(initial_state \\ %{}) do
    {:ok, state_pid} =
      Agent.start_link(fn ->
        Map.merge(
          %{
            objects: %{},
            requests: [],
            resumable_fail: false,
            paginate_rewrite: false,
            stream_chunks: ["chunk-1\n", "chunk-2\n", "chunk-3\n"],
            private_key: nil
          },
          initial_state
        )
      end)

    ref = make_ref()

    _server =
      start_supervised!(
        {Plug.Cowboy,
         ref: ref,
         scheme: :http,
         plug: {__MODULE__.MockGCSPlug, %{state_pid: state_pid}},
         options: [ip: {127, 0, 0, 1}, port: 0]}
      )

    {{127, 0, 0, 1}, port} = :ranch.get_addr(ref)
    base_url = "http://127.0.0.1:#{port}"

    %{base_url: base_url, state_pid: state_pid, port: port}
  end

  defmodule MockGCSPlug do
    import Plug.Conn

    def init(opts), do: opts

    def call(conn, %{state_pid: state_pid}) do
      conn = fetch_query_params(conn)

      Agent.update(state_pid, fn state ->
        req = %{
          method: conn.method,
          path: conn.request_path,
          query_params: conn.query_params,
          req_headers: Map.new(conn.req_headers)
        }

        %{state | requests: [req | state.requests]}
      end)

      handle_request(conn.method, conn.path_info, conn, state_pid)
    end

    defp handle_request(
           "GET",
           ["computeMetadata", "v1", "instance", "service-accounts", "default", "email"],
           conn,
           state_pid
         ) do
      ["Google"] = get_req_header(conn, "metadata-flavor")
      email = Agent.get(state_pid, & &1[:service_account_email])
      send_resp(conn, 200, email)
    end

    defp handle_request(
           "GET",
           ["computeMetadata", "v1", "instance", "service-accounts", "default", "token"],
           conn,
           _state_pid
         ) do
      ["Google"] = get_req_header(conn, "metadata-flavor")
      send_resp(conn, 200, Jason.encode!(%{"access_token" => "metadata-token-xyz"}))
    end

    defp handle_request(
           "POST",
           ["v1", "projects", "-", "serviceAccounts", email_and_action],
           conn,
           state_pid
         ) do
      true = String.ends_with?(email_and_action, ":signBlob")
      {:ok, body, conn} = read_body(conn)
      %{"payload" => payload_b64} = Jason.decode!(body)
      {:ok, string_to_sign} = Base.decode64(payload_b64)
      rsa_key = Agent.get(state_pid, & &1.private_key)
      sig = :public_key.sign(string_to_sign, :sha256, rsa_key)

      send_resp(conn, 200, Jason.encode!(%{"signedBlob" => Base.encode64(sig)}))
    end

    defp handle_request(
           "POST",
           ["upload", "storage", "v1", "b", bucket, "o"],
           conn,
           state_pid
         ) do
      if conn.query_params["uploadType"] == "resumable" do
        if Agent.get(state_pid, & &1.resumable_fail) do
          send_resp(conn, 500, "resumable error")
        else
          name = conn.query_params["name"]
          scheme = Atom.to_string(conn.scheme)

          session_url =
            "#{scheme}://#{conn.host}:#{conn.port}/upload/storage/v1/b/#{bucket}/o?uploadType=resumable&name=#{URI.encode_www_form(name)}&upload_id=session-123"

          conn
          |> put_resp_header("location", session_url)
          |> send_resp(200, "")
        end
      else
        send_resp(conn, 400, "unsupported uploadType")
      end
    end

    defp handle_request(
           "PUT",
           ["upload", "storage", "v1", "b", bucket, "o"],
           conn,
           state_pid
         ) do
      {:ok, body, conn} = read_full_body(conn, "")
      name = conn.query_params["name"]
      content_type = get_req_header(conn, "content-type") |> List.first("application/octet-stream")

      Agent.update(state_pid, fn state ->
        put_in(state, [:objects, {bucket, name}], %{
          data: body,
          size: byte_size(body),
          content_type: content_type
        })
      end)

      send_resp(
        conn,
        200,
        Jason.encode!(%{
          "name" => name,
          "size" => Integer.to_string(byte_size(body)),
          "contentType" => content_type
        })
      )
    end

    defp handle_request(
           "POST",
           ["storage", "v1", "b", src_bucket, "o", src_obj, "rewriteTo", "b", dst_bucket, "o", dst_obj],
           conn,
           state_pid
         ) do
      src_key = URI.decode(src_obj)
      dst_key = URI.decode(dst_obj)

      state = Agent.get(state_pid, & &1)

      case Map.get(state.objects, {src_bucket, src_key}) do
        nil ->
          send_resp(conn, 404, Jason.encode!(%{"error" => "not found"}))

        obj ->
          if state.paginate_rewrite and conn.query_params["rewriteToken"] == nil do
            send_resp(
              conn,
              200,
              Jason.encode!(%{"done" => false, "rewriteToken" => "tok-page-2"})
            )
          else
            Agent.update(state_pid, fn s ->
              put_in(s, [:objects, {dst_bucket, dst_key}], obj)
            end)

            send_resp(conn, 200, Jason.encode!(%{"done" => true}))
          end
      end
    end

    defp handle_request(
           "DELETE",
           ["storage", "v1", "b", bucket, "o", obj],
           conn,
           state_pid
         ) do
      key = URI.decode(obj)

      existed? =
        Agent.get_and_update(state_pid, fn state ->
          {Map.has_key?(state.objects, {bucket, key}),
           %{state | objects: Map.delete(state.objects, {bucket, key})}}
        end)

      if existed? do
        send_resp(conn, 204, "")
      else
        send_resp(conn, 404, "not found")
      end
    end

    defp handle_request(
           "GET",
           ["storage", "v1", "b", bucket, "o", obj],
           conn,
           state_pid
         ) do
      key = URI.decode(obj)
      state = Agent.get(state_pid, & &1)

      case Map.get(state.objects, {bucket, key}) do
        nil ->
          send_resp(conn, 404, "not found")

        %{data: data, size: size, content_type: content_type} ->
          if conn.query_params["alt"] == "media" do
            conn =
              conn
              |> put_resp_content_type(content_type)
              |> send_chunked(200)

            chunks =
              for <<chunk::binary-size(32) <- data>> do
                chunk
              end

            rem_size = rem(byte_size(data), 32)

            chunks =
              if rem_size > 0 do
                chunks ++ [binary_part(data, byte_size(data) - rem_size, rem_size)]
              else
                chunks
              end

            Enum.reduce_while(chunks, conn, fn chunk, acc_conn ->
              case chunk(acc_conn, chunk) do
                {:ok, next_conn} -> {:cont, next_conn}
                {:error, _reason} -> {:halt, acc_conn}
              end
            end)
          else
            send_resp(
              conn,
              200,
              Jason.encode!(%{
                "name" => key,
                "size" => Integer.to_string(size),
                "contentType" => content_type
              })
            )
          end
      end
    end

    defp read_full_body(conn, acc) do
      case read_body(conn) do
        {:ok, chunk, conn} -> {:ok, acc <> chunk, conn}
        {:more, chunk, conn} -> read_full_body(conn, acc <> chunk)
      end
    end
  end
end
