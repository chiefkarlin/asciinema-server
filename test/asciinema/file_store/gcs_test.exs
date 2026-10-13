defmodule Asciinema.FileStore.GCSTest do
  use ExUnit.Case, async: true
  alias Asciinema.AppEnv
  alias Asciinema.FileStore
  alias Asciinema.FileStore.GCS

  setup_all do
    rsa_key = :public_key.generate_key({:rsa, 2048, 65537})
    pem_entry = :public_key.pem_entry_encode(:RSAPrivateKey, rsa_key)
    private_key_pem = :public_key.pem_encode([pem_entry])
    {:RSAPrivateKey, _, modulus, pub_exp, _, _, _, _, _, _, _} = rsa_key
    public_key = {:RSAPublicKey, modulus, pub_exp}

    %{
      rsa_key: rsa_key,
      public_key: public_key,
      private_key_pem: private_key_pem,
      client_email: "asciinema-test@northam-ce-mlai-tpu.iam.gserviceaccount.com"
    }
  end

  def start_mock_gcs_for_test(initial_state \\ %{}),
    do: Asciinema.GcsTestHelpers.start_mock_gcs(initial_state)

  defp start_mock_gcs(initial_state \\ %{}),
    do: Asciinema.GcsTestHelpers.start_mock_gcs(initial_state)

  describe "V4 URL signing (direct_download_url/2 & uri/1)" do
    test "signs GET URL locally with RSA private key and verifies signature", %{
      private_key_pem: pem,
      public_key: public_key,
      client_email: email
    } do
      AppEnv.put(FileStore, adapter: GCS)

      AppEnv.put(GCS,
        bucket: "test-asciinema-bucket",
        path: "uploads/",
        storage_base_url: "https://storage.googleapis.com",
        credentials: %{
          "client_email" => email,
          "private_key" => pem
        }
      )

      now = ~U[2026-10-12 12:00:00Z]

      assert {:ok, signed_url} =
               FileStore.direct_download_url("recordings/alice/123.cast",
                 filename: "123.cast",
                 response_content_type: "application/x-asciicast",
                 expires: 900,
                 now: now
               )

      uri = URI.parse(signed_url)
      assert uri.scheme == "https"
      assert uri.host == "storage.googleapis.com"
      assert uri.path == "/test-asciinema-bucket/uploads/recordings/alice/123.cast"

      query = URI.decode_query(uri.query)
      assert query["X-Goog-Algorithm"] == "GOOG4-RSA-SHA256"

      assert query["X-Goog-Credential"] ==
               "#{email}/20261012/auto/storage/goog4_request"

      assert query["X-Goog-Date"] == "20261012T120000Z"
      assert query["X-Goog-Expires"] == "900"
      assert query["X-Goog-SignedHeaders"] == "host"
      assert query["response-content-type"] == "application/x-asciicast"
      assert query["response-content-disposition"] == ~s(attachment; filename="123.cast")

      sig_hex = query["X-Goog-Signature"]
      {:ok, sig_bytes} = Base.decode16(sig_hex, case: :lower)

      [canonical_query, _sig_part] = String.split(uri.query, "&X-Goog-Signature=")

      canonical_request =
        [
          "GET",
          uri.path,
          canonical_query,
          "host:storage.googleapis.com\n",
          "host",
          "UNSIGNED-PAYLOAD"
        ]
        |> Enum.join("\n")

      hashed_req = :crypto.hash(:sha256, canonical_request) |> Base.encode16(case: :lower)

      string_to_sign =
        [
          "GOOG4-RSA-SHA256",
          "20261012T120000Z",
          "20261012/auto/storage/goog4_request",
          hashed_req
        ]
        |> Enum.join("\n")

      assert :public_key.verify(string_to_sign, :sha256, sig_bytes, public_key)
    end

    test "signs V4 URL via GCP Metadata Server + IAM signBlob API when no local private key is set",
         %{rsa_key: rsa_key, public_key: public_key, client_email: email} do
      %{base_url: base_url, state_pid: state_pid} =
        start_mock_gcs(%{private_key: rsa_key, service_account_email: email})

      AppEnv.put(FileStore, adapter: GCS)

      AppEnv.put(GCS,
        bucket: "cloudrun-bucket",
        path: "uploads/",
        storage_base_url: base_url,
        metadata_url: base_url <> "/computeMetadata/v1",
        iam_credentials_url: base_url
      )

      now = ~U[2026-10-12 15:30:00Z]

      assert {:ok, signed_url} =
               FileStore.direct_download_url("recordings/bob/456.cast",
                 now: now,
                 expires: 600
               )

      uri = URI.parse(signed_url)
      query = URI.decode_query(uri.query)
      assert query["X-Goog-Algorithm"] == "GOOG4-RSA-SHA256"
      assert query["X-Goog-Credential"] =~ email

      sig_bytes = Base.decode16!(query["X-Goog-Signature"], case: :lower)
      [canonical_query, _] = String.split(uri.query, "&X-Goog-Signature=")

      canonical_request =
        [
          "GET",
          uri.path,
          canonical_query,
          "host:#{uri.host}:#{uri.port}\n",
          "host",
          "UNSIGNED-PAYLOAD"
        ]
        |> Enum.join("\n")

      hashed_req = :crypto.hash(:sha256, canonical_request) |> Base.encode16(case: :lower)

      string_to_sign =
        [
          "GOOG4-RSA-SHA256",
          "20261012T153000Z",
          "20261012/auto/storage/goog4_request",
          hashed_req
        ]
        |> Enum.join("\n")

      assert :public_key.verify(string_to_sign, :sha256, sig_bytes, public_key)

      requests = Agent.get(state_pid, & &1.requests)
      assert Enum.any?(requests, &(&1.path == "/computeMetadata/v1/instance/service-accounts/default/email"))
      assert Enum.any?(requests, &(&1.path == "/computeMetadata/v1/instance/service-accounts/default/token"))
      assert Enum.any?(requests, &String.ends_with?(&1.path, ":signBlob"))
    end
  end

  describe "initiate_direct_upload/2" do
    test "initiates resumable upload session and returns session URI", %{
      private_key_pem: pem,
      client_email: email
    } do
      %{base_url: base_url} = start_mock_gcs()

      AppEnv.put(FileStore, adapter: GCS)

      AppEnv.put(GCS,
        bucket: "test-bucket",
        path: "uploads/",
        storage_base_url: base_url,
        access_token: "test-oauth-token",
        credentials: %{"client_email" => email, "private_key" => pem}
      )

      assert {:ok, spec} =
               FileStore.initiate_direct_upload("tmp/direct-uploads/abc.cast",
                 content_type: "application/x-asciicast",
                 byte_size: 1_073_741_824
               )

      assert spec.upload_type == "resumable"
      assert spec.upload_method == "PUT"
      assert spec.object_key == "tmp/direct-uploads/abc.cast"
      assert spec.headers == %{"Content-Type" => "application/x-asciicast"}
      assert spec.upload_url =~ "#{base_url}/upload/storage/v1/b/test-bucket/o"
      assert spec.upload_url =~ "upload_id=session-123"
    end

    test "falls back to V4 signed PUT URL when resumable initiation fails", %{
      private_key_pem: pem,
      client_email: email
    } do
      %{base_url: base_url} = start_mock_gcs(%{resumable_fail: true})

      AppEnv.put(FileStore, adapter: GCS)

      AppEnv.put(GCS,
        bucket: "test-bucket",
        path: "uploads/",
        storage_base_url: base_url,
        access_token: "test-oauth-token",
        credentials: %{"client_email" => email, "private_key" => pem}
      )

      assert {:ok, spec} =
               FileStore.initiate_direct_upload("tmp/direct-uploads/fallback.cast",
                 content_type: "application/x-asciicast"
               )

      assert spec.upload_type == "signed_put"
      assert spec.upload_method == "PUT"
      assert spec.upload_url =~ "X-Goog-Algorithm=GOOG4-RSA-SHA256"
    end
  end

  describe "put_file/3, stat_file/1, move_file/2, delete_file/1, and open_stream/2" do
    test "streams upload in chunks, stats object, rewrites/moves server-side, streams download, and deletes",
         %{private_key_pem: pem, client_email: email} do
      %{base_url: base_url, state_pid: state_pid} = start_mock_gcs()

      AppEnv.put(FileStore, adapter: GCS)

      AppEnv.put(GCS,
        bucket: "test-bucket",
        path: "uploads/",
        storage_base_url: base_url,
        access_token: "test-oauth-token",
        chunk_size: 16,
        credentials: %{"client_email" => email, "private_key" => pem}
      )

      local_path = Briefly.create!()
      payload = String.duplicate("0123456789abcdef\n", 20)
      File.write!(local_path, payload)

      # 1. put_file streams to uploadType=media
      assert :ok =
               FileStore.put_file(
                 "tmp/staged.cast",
                 local_path,
                 "application/x-asciicast"
               )

      # 2. stat_file returns size and content_type
      assert {:ok, %{size: size, content_type: "application/x-asciicast"}} =
               FileStore.stat_file("tmp/staged.cast")

      assert size == byte_size(payload)

      # 3. open_stream lazily streams chunks from GCS
      assert {:ok, stream} = FileStore.open_stream("tmp/staged.cast")
      assert Enum.join(stream) == payload

      # Early halt on open_stream closes cleanly
      assert {:ok, stream2} = FileStore.open_stream("tmp/staged.cast")
      assert [first_chunk] = Enum.take(stream2, 1)
      assert is_binary(first_chunk)

      # 4. move_file performs server-side rewriteTo (including rewriteToken pagination) + DELETE
      Agent.update(state_pid, &Map.put(&1, :paginate_rewrite, true))
      assert :ok = FileStore.move_file("tmp/staged.cast", "recordings/user/final.cast")

      assert {:error, :enoent} = FileStore.stat_file("tmp/staged.cast")

      assert {:ok, %{size: final_size}} = FileStore.stat_file("recordings/user/final.cast")
      assert final_size == byte_size(payload)

      # 5. delete_file removes object and returns {:error, :enoent} when missing
      assert :ok = FileStore.delete_file("recordings/user/final.cast")
      assert {:error, :enoent} = FileStore.delete_file("recordings/user/final.cast")
      assert {:error, :enoent} = FileStore.open_stream("recordings/user/final.cast")
    end
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
