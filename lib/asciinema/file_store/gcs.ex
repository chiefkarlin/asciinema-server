defmodule Asciinema.FileStore.GCS do
  @moduledoc """
  Google Cloud Storage adapter for `Asciinema.FileStore`.

  Supports:
  - V4 Signed URLs (`GOOG4-RSA-SHA256`) via either local Service Account RSA private key
    (`:public_key.sign/3`) or Cloud Run GCP Metadata Server + IAM Credentials `signBlob` API.
  - Direct upload initiation via GCS Resumable Upload sessions (`uploadType=resumable`)
    with automatic or configurable fallback to V4 Signed `PUT` URLs.
  - Bounded-memory streaming uploads (`put_file/3`) in 64 KB chunks via `:hackney`
    streaming body without `File.read!/1`.
  - Lazy HTTP chunk streaming (`open_stream/2`) via `Stream.resource/3` and `:hackney`
    with per-chunk `recv_timeout` and guaranteed socket cleanup on early halt.
  - Zero-copy server-side object moves (`move_file/2`) via GCS `rewriteTo` + `DELETE`.
  - Configurable `storage_base_url` (`GCS_ENDPOINT`), `metadata_url`, and `iam_credentials_url`
    for deterministic offline unit/integration testing with `Plug.Cowboy`.
  """

  use Asciinema.Config
  require Logger

  @behaviour Asciinema.FileStore

  @default_storage_base_url "https://storage.googleapis.com"
  @default_metadata_url "http://metadata.google.internal/computeMetadata/v1"
  @default_iam_credentials_url "https://iamcredentials.googleapis.com"
  @default_token_uri "https://oauth2.googleapis.com/token"
  @default_chunk_size 65_536
  @default_signed_url_expires 3_600
  @default_connect_timeout 10_000
  @default_recv_timeout 60_000

  @impl true
  def direct_upload_supported? do
    config(:direct_upload, true)
  end

  @impl true
  def direct_download_supported? do
    config(:direct_download, true)
  end

  @impl true
  def uri(path) do
    case direct_download_url(path, []) do
      {:ok, url} -> url
      {:error, reason} -> raise "failed to generate GCS URI for #{path}: #{inspect(reason)}"
    end
  end

  @impl true
  def direct_download_url(path, opts \\ []) do
    expires = Keyword.get(opts, :expires, config(:signed_url_expires, @default_signed_url_expires))
    response_content_type = Keyword.get(opts, :response_content_type, "application/x-asciicast")
    filename = Keyword.get(opts, :filename)

    query_params =
      %{}
      |> maybe_put_param("response-content-type", response_content_type)
      |> maybe_put_disposition(filename)

    sign_v4_url("GET", full_key(path),
      expires: expires,
      query_params: query_params,
      now: Keyword.get(opts, :now)
    )
  end

  @impl true
  def initiate_direct_upload(dst_path, opts \\ []) do
    content_type = Keyword.get(opts, :content_type, "application/x-asciicast")
    upload_type = Keyword.get(opts, :upload_type, config(:direct_upload_type, :resumable))
    expires = Keyword.get(opts, :expires, config(:signed_url_expires, @default_signed_url_expires))
    key = full_key(dst_path)

    case upload_type do
      :resumable ->
        case start_resumable_session(key, content_type, opts) do
          {:ok, session_uri} ->
            {:ok,
             %{
               upload_url: session_uri,
               upload_method: "PUT",
               upload_type: "resumable",
               headers: %{"Content-Type" => content_type},
               object_key: dst_path
             }}

          {:error, reason} ->
            if Keyword.get(opts, :fallback_to_signed_put, true) do
              Logger.warning(
                "GCS resumable session initiation failed (#{inspect(reason)}), falling back to V4 signed PUT URL"
              )

              build_signed_put_upload(dst_path, key, content_type, expires, opts)
            else
              {:error, reason}
            end
        end

      :signed_put ->
        build_signed_put_upload(dst_path, key, content_type, expires, opts)
    end
  end

  defp build_signed_put_upload(dst_path, key, content_type, expires, opts) do
    with {:ok, signed_url} <-
           sign_v4_url("PUT", key,
             expires: expires,
             now: Keyword.get(opts, :now)
           ) do
      {:ok,
       %{
         upload_url: signed_url,
         upload_method: "PUT",
         upload_type: "signed_put",
         headers: %{"Content-Type" => content_type},
         object_key: dst_path
       }}
    end
  end

  @doc """
  Initiates a GCS Resumable Upload session via:
  `POST {storage_base_url}/upload/storage/v1/b/{bucket}/o?uploadType=resumable&name={encoded_key}`
  and extracts the `Location` response header.
  """
  def start_resumable_session(full_object_key, content_type, opts \\ []) do
    with {:ok, token} <- fetch_access_token() do
      url =
        "#{storage_base_url()}/upload/storage/v1/b/#{uri_encode(bucket())}/o" <>
          "?uploadType=resumable&name=#{uri_encode(full_object_key)}"

      headers =
        [
          {"authorization", "Bearer #{token}"},
          {"content-type", "application/json; charset=UTF-8"},
          {"x-upload-content-type", content_type}
        ]
        |> maybe_put_upload_length(Keyword.get(opts, :byte_size))

      body = Jason.encode!(%{"name" => full_object_key, "contentType" => content_type})

      case hackney_request(:post, url, headers, body) do
        {:ok, status, resp_headers, _resp_body} when status in [200, 201] ->
          case get_header(resp_headers, "location") do
            location when is_binary(location) and location != "" ->
              {:ok, location}

            _ ->
              {:error, :missing_resumable_location}
          end

        {:ok, status, _resp_headers, resp_body} ->
          {:error, {:http_status, status, resp_body}}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @impl true
  def put_file(dst_path, src_local_path, content_type) do
    with {:ok, %{size: file_size}} <- File.stat(src_local_path),
          {:ok, token} <- fetch_access_token() do
      key = full_key(dst_path)

      url =
        "#{storage_base_url()}/upload/storage/v1/b/#{uri_encode(bucket())}/o" <>
          "?uploadType=media&name=#{uri_encode(key)}"

      headers = [
        {"authorization", "Bearer #{token}"},
        {"content-type", content_type},
        {"content-length", Integer.to_string(file_size)}
      ]

      options = [
        connect_timeout: connect_timeout(),
        recv_timeout: recv_timeout()
      ]

      case :hackney.request(:post, url, headers, :stream, options) do
        {:ok, client_ref} ->
          stream_file_to_hackney(client_ref, src_local_path)

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp stream_file_to_hackney(client_ref, src_local_path) do
    chunk_size = config(:chunk_size, @default_chunk_size)

    send_result =
      try do
        src_local_path
        |> File.stream!(chunk_size)
        |> Enum.reduce_while(:ok, fn chunk, :ok ->
          case :hackney.send_body(client_ref, chunk) do
            :ok -> {:cont, :ok}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)
      rescue
        e -> {:error, e}
      end

    case send_result do
      :ok ->
        case :hackney.start_response(client_ref) do
          {:ok, status, _resp_headers, client_ref} when status in [200, 201] ->
            _ = :hackney.skip_body(client_ref)
            :ok

          {:ok, status, _resp_headers, client_ref} ->
            {:ok, resp_body} = :hackney.body(client_ref)
            {:error, {:http_status, status, resp_body}}

          {:error, reason} ->
            :hackney.close(client_ref)
            {:error, reason}
        end

      {:error, reason} ->
        :hackney.close(client_ref)
        {:error, reason}
    end
  end

  @impl true
  def move_file(from_path, to_path) do
    with {:ok, token} <- fetch_access_token(),
         :ok <- rewrite_object(full_key(from_path), full_key(to_path), token, nil) do
      case delete_file(from_path) do
        :ok -> :ok
        {:error, :enoent} -> :ok
        {:error, _} = err -> err
      end
    end
  end

  defp rewrite_object(src_key, dst_key, token, rewrite_token) do
    base_url =
      "#{storage_base_url()}/storage/v1/b/#{uri_encode(bucket())}/o/#{uri_encode(src_key)}" <>
        "/rewriteTo/b/#{uri_encode(bucket())}/o/#{uri_encode(dst_key)}"

    url =
      if rewrite_token do
        base_url <> "?rewriteToken=#{uri_encode(rewrite_token)}"
      else
        base_url
      end

    headers = [
      {"authorization", "Bearer #{token}"},
      {"content-type", "application/json"},
      {"content-length", "0"}
    ]

    case hackney_request(:post, url, headers, "") do
      {:ok, 200, _resp_headers, resp_body} ->
        case Jason.decode(resp_body) do
          {:ok, %{"done" => true}} ->
            :ok

          {:ok, %{"done" => false, "rewriteToken" => next_token}} when is_binary(next_token) ->
            rewrite_object(src_key, dst_key, token, next_token)

          {:ok, _} ->
            :ok

          {:error, _} ->
            :ok
        end

      {:ok, 404, _resp_headers, _resp_body} ->
        {:error, :enoent}

      {:ok, status, _resp_headers, resp_body} ->
        {:error, {:http_status, status, resp_body}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def delete_file(path) do
    with {:ok, token} <- fetch_access_token() do
      key = full_key(path)
      url = "#{storage_base_url()}/storage/v1/b/#{uri_encode(bucket())}/o/#{uri_encode(key)}"
      headers = [{"authorization", "Bearer #{token}"}]

      case hackney_request(:delete, url, headers, "") do
        {:ok, status, _resp_headers, _resp_body} when status in [200, 204] ->
          :ok

        {:ok, 404, _resp_headers, _resp_body} ->
          {:error, :enoent}

        {:ok, status, _resp_headers, resp_body} ->
          {:error, {:http_status, status, resp_body}}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @impl true
  def stat_file(path) do
    with {:ok, token} <- fetch_access_token() do
      key = full_key(path)

      url =
        "#{storage_base_url()}/storage/v1/b/#{uri_encode(bucket())}/o/#{uri_encode(key)}" <>
          "?fields=name,size,contentType"

      headers = [{"authorization", "Bearer #{token}"}]

      case hackney_request(:get, url, headers, "") do
        {:ok, 200, _resp_headers, resp_body} ->
          case Jason.decode(resp_body) do
            {:ok, %{"size" => size} = meta} ->
              parsed_size =
                cond do
                  is_integer(size) -> size
                  is_binary(size) -> String.to_integer(size)
                  true -> 0
                end

              {:ok, %{size: parsed_size, content_type: meta["contentType"]}}

            {:error, reason} ->
              {:error, reason}
          end

        {:ok, 404, _resp_headers, _resp_body} ->
          {:error, :enoent}

        {:ok, status, _resp_headers, resp_body} ->
          {:error, {:http_status, status, resp_body}}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  def head_object(path), do: stat_file(path)

  @impl true
  def open_stream(path, opts \\ []) do
    with {:ok, token} <- fetch_access_token() do
      key = full_key(path)

      url =
        "#{storage_base_url()}/storage/v1/b/#{uri_encode(bucket())}/o/#{uri_encode(key)}?alt=media"

      headers = [
        {"authorization", "Bearer #{token}"},
        {"accept-encoding", "identity"}
      ]

      stream_http_url(url, Keyword.put(opts, :headers, headers))
    end
  end

  def stream!(path, opts \\ []) do
    case open_stream(path, opts) do
      {:ok, stream} -> stream
      {:error, reason} -> raise "failed to open GCS stream for #{path}: #{inspect(reason)}"
    end
  end

  @doc """
  Opens an HTTP GET connection via `:hackney` with `async: :once` (`{active, once}`
  socket framing) and returns `{:ok, Enumerable.t()}` that lazily streams response
  body chunks via `Stream.resource/3` in bounded memory regardless of `Content-Length`.
  Verifies HTTP 200 status upfront before returning `{:ok, stream}` so 404s
  immediately return `{:error, :enoent}`.
  """
  def stream_http_url(url, opts \\ []) do
    headers = Keyword.get(opts, :headers, [{"accept-encoding", "identity"}])
    connect_timeout = Keyword.get(opts, :connect_timeout, connect_timeout())
    recv_timeout = Keyword.get(opts, :recv_timeout, recv_timeout())
    max_redirects = Keyword.get(opts, :max_redirects, 5)

    options = [
      connect_timeout: connect_timeout,
      recv_timeout: recv_timeout,
      follow_redirect: true,
      async: :once
    ]

    case :hackney.request(:get, url, headers, <<>>, options) do
      {:ok, client_ref} ->
        await_stream_status(client_ref, url, opts, recv_timeout, max_redirects)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp await_stream_status(ref, url, opts, recv_timeout, max_redirects) do
    receive do
      {:hackney_response, ^ref, {:status, status, _reason}} ->
        case await_stream_headers(ref, recv_timeout) do
          {:ok, _resp_headers} ->
            handle_stream_status(ref, status, recv_timeout)

          {:error, reason} ->
            :hackney.close(ref)
            flush_hackney_messages(ref)
            {:error, reason}
        end

      {:hackney_response, ^ref, {:redirect, to_url, _headers}} ->
        flush_hackney_messages(ref)

        if max_redirects > 0 do
          next_url = URI.merge(url, to_url) |> URI.to_string()
          headers = Keyword.get(opts, :headers, [{"accept-encoding", "identity"}])

          next_headers =
            if String.contains?(next_url, "X-Goog-Algorithm=") or
                 URI.parse(next_url).host != URI.parse(url).host do
              Enum.reject(headers, fn {k, _v} -> String.downcase(k) == "authorization" end)
            else
              headers
            end

          next_opts =
            opts
            |> Keyword.put(:headers, next_headers)
            |> Keyword.put(:max_redirects, max_redirects - 1)

          stream_http_url(next_url, next_opts)
        else
          {:error, :too_many_redirects}
        end

      {:hackney_response, ^ref, {:error, reason}} ->
        :hackney.close(ref)
        flush_hackney_messages(ref)
        {:error, reason}
    after
      recv_timeout ->
        :hackney.close(ref)
        flush_hackney_messages(ref)
        {:error, :timeout}
    end
  end

  defp await_stream_headers(ref, recv_timeout) do
    :hackney.stream_next(ref)

    receive do
      {:hackney_response, ^ref, {:headers, resp_headers}} ->
        {:ok, resp_headers}

      {:hackney_response, ^ref, {:error, reason}} ->
        {:error, reason}
    after
      recv_timeout ->
        {:error, :timeout}
    end
  end

  defp handle_stream_status(ref, 200, recv_timeout) do
    stream =
      Stream.resource(
        fn -> ref end,
        fn
          :done ->
            {:halt, :done}

          active_ref ->
            :hackney.stream_next(active_ref)

            receive do
              {:hackney_response, ^active_ref, :done} ->
                {:halt, :done}

              {:hackney_response, ^active_ref, chunk} when is_binary(chunk) ->
                {[chunk], active_ref}

              {:hackney_response, ^active_ref, {:error, reason}} ->
                raise "GCS HTTP stream error: #{inspect(reason)}"
            after
              recv_timeout ->
                raise "GCS HTTP stream error: :timeout"
            end
        end,
        fn
          :done ->
            flush_hackney_messages(ref)

          active_ref ->
            :hackney.close(active_ref)
            flush_hackney_messages(active_ref)
        end
      )

    {:ok, stream}
  end

  defp handle_stream_status(ref, 404, _recv_timeout) do
    :hackney.close(ref)
    flush_hackney_messages(ref)
    {:error, :enoent}
  end

  defp handle_stream_status(ref, status, recv_timeout) do
    body = read_async_error_body(ref, recv_timeout, [], 0)
    :hackney.close(ref)
    flush_hackney_messages(ref)
    {:error, {:http_status, status, body}}
  end

  defp read_async_error_body(_ref, _recv_timeout, acc, size) when size >= 65_536 do
    acc |> Enum.reverse() |> IO.iodata_to_binary()
  end

  defp read_async_error_body(ref, recv_timeout, acc, size) do
    :hackney.stream_next(ref)

    receive do
      {:hackney_response, ^ref, :done} ->
        acc |> Enum.reverse() |> IO.iodata_to_binary()

      {:hackney_response, ^ref, chunk} when is_binary(chunk) ->
        read_async_error_body(ref, recv_timeout, [chunk | acc], size + byte_size(chunk))

      {:hackney_response, ^ref, {:error, _reason}} ->
        acc |> Enum.reverse() |> IO.iodata_to_binary()
    after
      recv_timeout ->
        acc |> Enum.reverse() |> IO.iodata_to_binary()
    end
  end

  defp flush_hackney_messages(ref) do
    receive do
      {:hackney_response, ^ref, _} -> flush_hackney_messages(ref)
    after
      0 -> :ok
    end
  end

  # ===========================================================================
  # V4 Signed URL Generation (`GOOG4-RSA-SHA256`)
  # ===========================================================================

  @doc """
  Generates a GCS V4 Signed URL (`GOOG4-RSA-SHA256`) for `http_method` (`"GET"` or `"PUT"`)
  and `full_object_key` inside `bucket()`.
  """
  def sign_v4_url(http_method, full_object_key, opts \\ []) do
    expires = Keyword.get(opts, :expires, @default_signed_url_expires)
    extra_query_params = Keyword.get(opts, :query_params, %{})
    now = Keyword.get(opts, :now) || DateTime.utc_now()

    with {:ok, signer} <- resolve_signer() do
      base_url = storage_base_url()
      uri = URI.parse(base_url)
      host_header = format_host_header(uri)
      canonical_uri = "/#{encode_object_path(bucket())}/#{encode_object_path(full_object_key)}"

      request_timestamp = Calendar.strftime(now, "%Y%m%dT%H%M%SZ")
      datestamp = Calendar.strftime(now, "%Y%m%d")
      credential_scope = "#{datestamp}/auto/storage/goog4_request"
      credential = "#{signer.client_email}/#{credential_scope}"

      signed_headers = "host"
      canonical_headers = "host:#{host_header}\n"

      query_params =
        Map.merge(extra_query_params, %{
          "X-Goog-Algorithm" => "GOOG4-RSA-SHA256",
          "X-Goog-Credential" => credential,
          "X-Goog-Date" => request_timestamp,
          "X-Goog-Expires" => Integer.to_string(expires),
          "X-Goog-SignedHeaders" => signed_headers
        })

      canonical_query_string =
        query_params
        |> Enum.sort_by(fn {k, _v} -> k end)
        |> Enum.map_join("&", fn {k, v} -> "#{uri_encode(k)}=#{uri_encode(v)}" end)

      canonical_request =
        [
          String.upcase(to_string(http_method)),
          canonical_uri,
          canonical_query_string,
          canonical_headers,
          signed_headers,
          "UNSIGNED-PAYLOAD"
        ]
        |> Enum.join("\n")

      hashed_canonical_request =
        :crypto.hash(:sha256, canonical_request)
        |> Base.encode16(case: :lower)

      string_to_sign =
        [
          "GOOG4-RSA-SHA256",
          request_timestamp,
          credential_scope,
          hashed_canonical_request
        ]
        |> Enum.join("\n")

      with {:ok, signature_hex} <- sign_string(signer, string_to_sign) do
        signed_url =
          "#{base_url}#{canonical_uri}?#{canonical_query_string}&X-Goog-Signature=#{signature_hex}"

        {:ok, signed_url}
      end
    end
  end

  # ===========================================================================
  # Credential Resolution & Signing (Local RSA Private Key vs IAM `signBlob`)
  # ===========================================================================

  defp resolve_signer do
    case load_service_account_credentials() do
      {:ok, %{"client_email" => email, "private_key" => pem}}
      when is_binary(email) and is_binary(pem) ->
        {:ok, %{type: :local_key, client_email: email, private_key_pem: pem}}

      :none ->
        with {:ok, email} <- fetch_service_account_email() do
          {:ok, %{type: :iam_sign_blob, client_email: email}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp sign_string(%{type: :local_key, private_key_pem: pem}, string_to_sign) do
    try do
      [pem_entry | _] = :public_key.pem_decode(pem)
      private_key = :public_key.pem_entry_decode(pem_entry)
      signature = :public_key.sign(string_to_sign, :sha256, private_key)
      {:ok, Base.encode16(signature, case: :lower)}
    rescue
      e -> {:error, {:rsa_sign_failed, e}}
    end
  end

  defp sign_string(%{type: :iam_sign_blob, client_email: email}, string_to_sign) do
    with {:ok, token} <- fetch_access_token() do
      url =
        "#{iam_credentials_url()}/v1/projects/-/serviceAccounts/#{uri_encode(email)}:signBlob"

      headers = [
        {"authorization", "Bearer #{token}"},
        {"content-type", "application/json"}
      ]

      body = Jason.encode!(%{"payload" => Base.encode64(string_to_sign)})

      case hackney_request(:post, url, headers, body) do
        {:ok, 200, _resp_headers, resp_body} ->
          with {:ok, %{"signedBlob" => signed_b64}} <- Jason.decode(resp_body),
               {:ok, raw_sig} <- Base.decode64(signed_b64) do
            {:ok, Base.encode16(raw_sig, case: :lower)}
          else
            _ -> {:error, :invalid_sign_blob_response}
          end

        {:ok, status, _resp_headers, resp_body} ->
          {:error, {:sign_blob_http_status, status, resp_body}}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp load_service_account_credentials do
    cond do
      is_map(config(:credentials)) ->
        {:ok, stringify_keys(config(:credentials))}

      is_binary(config(:credentials_json)) and config(:credentials_json) != "" ->
        Jason.decode(config(:credentials_json))

      is_binary(config(:credentials_path)) and config(:credentials_path) != "" ->
        with {:ok, content} <- File.read(config(:credentials_path)) do
          Jason.decode(content)
        end

      true ->
        :none
    end
  end

  defp fetch_service_account_email do
    case config(:service_account_email) do
      email when is_binary(email) and email != "" ->
        {:ok, email}

      _ ->
        url = "#{metadata_url()}/instance/service-accounts/default/email"
        headers = [{"metadata-flavor", "Google"}]

        case hackney_request(:get, url, headers, "") do
          {:ok, 200, _resp_headers, body} ->
            {:ok, String.trim(body)}

          {:ok, status, _resp_headers, body} ->
            {:error, {:metadata_email_http_status, status, body}}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  defp fetch_access_token do
    cond do
      is_binary(config(:access_token)) and config(:access_token) != "" ->
        {:ok, config(:access_token)}

      match?({:ok, %{"private_key" => _, "client_email" => _}}, load_service_account_credentials()) ->
        {:ok, creds} = load_service_account_credentials()
        fetch_service_account_jwt_token(creds)

      true ->
        fetch_metadata_access_token()
    end
  end

  defp fetch_metadata_access_token do
    url = "#{metadata_url()}/instance/service-accounts/default/token"
    headers = [{"metadata-flavor", "Google"}]

    case hackney_request(:get, url, headers, "") do
      {:ok, 200, _resp_headers, body} ->
        case Jason.decode(body) do
          {:ok, %{"access_token" => token}} when is_binary(token) ->
            {:ok, token}

          _ ->
            {:error, :invalid_metadata_token_response}
        end

      {:ok, status, _resp_headers, body} ->
        {:error, {:metadata_token_http_status, status, body}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp fetch_service_account_jwt_token(creds) do
    token_uri = Map.get(creds, "token_uri", @default_token_uri)
    now = System.system_time(:second)

    header_b64 =
      %{"alg" => "RS256", "typ" => "JWT"}
      |> Jason.encode!()
      |> Base.url_encode64(padding: false)

    claims_b64 =
      %{
        "iss" => creds["client_email"],
        "scope" =>
          "https://www.googleapis.com/auth/devstorage.full_control https://www.googleapis.com/auth/cloud-platform",
        "aud" => token_uri,
        "iat" => now,
        "exp" => now + 3600
      }
      |> Jason.encode!()
      |> Base.url_encode64(padding: false)

    signing_input = "#{header_b64}.#{claims_b64}"
    [pem_entry | _] = :public_key.pem_decode(creds["private_key"])
    private_key = :public_key.pem_entry_decode(pem_entry)

    sig_b64 =
      :public_key.sign(signing_input, :sha256, private_key)
      |> Base.url_encode64(padding: false)

    assertion = "#{signing_input}.#{sig_b64}"

    body =
      URI.encode_query(%{
        "grant_type" => "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion" => assertion
      })

    headers = [{"content-type", "application/x-www-form-urlencoded"}]

    case hackney_request(:post, token_uri, headers, body) do
      {:ok, 200, _resp_headers, resp_body} ->
        case Jason.decode(resp_body) do
          {:ok, %{"access_token" => token}} -> {:ok, token}
          _ -> {:error, :invalid_oauth_token_response}
        end

      {:ok, status, _resp_headers, resp_body} ->
        {:error, {:oauth_token_http_status, status, resp_body}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ===========================================================================
  # Internal Helpers
  # ===========================================================================

  defp hackney_request(method, url, headers, body) do
    options = [
      connect_timeout: connect_timeout(),
      recv_timeout: recv_timeout(),
      with_body: true
    ]

    :hackney.request(method, url, headers, body, options)
  end

  defp bucket do
    config(:bucket) || raise "Asciinema.FileStore.GCS :bucket is not configured"
  end

  defp base_path do
    config(:path, "uploads/")
  end

  defp full_key(path) do
    prefix = base_path()

    cond do
      prefix in [nil, ""] -> String.trim_leading(path, "/")
      String.ends_with?(prefix, "/") -> prefix <> String.trim_leading(path, "/")
      true -> prefix <> "/" <> String.trim_leading(path, "/")
    end
  end

  defp storage_base_url do
    config(:storage_base_url, @default_storage_base_url)
    |> String.trim_trailing("/")
  end

  defp metadata_url do
    config(:metadata_url, @default_metadata_url)
    |> String.trim_trailing("/")
  end

  defp iam_credentials_url do
    config(:iam_credentials_url, @default_iam_credentials_url)
    |> String.trim_trailing("/")
  end

  defp connect_timeout, do: config(:connect_timeout, @default_connect_timeout)
  defp recv_timeout, do: config(:recv_timeout, @default_recv_timeout)

  defp format_host_header(%URI{scheme: "http", host: host, port: port})
       when is_integer(port) and port != 80,
       do: "#{host}:#{port}"

  defp format_host_header(%URI{scheme: "https", host: host, port: port})
       when is_integer(port) and port != 443,
       do: "#{host}:#{port}"

  defp format_host_header(%URI{host: host}), do: host

  defp encode_object_path(path) do
    path
    |> String.trim_leading("/")
    |> String.split("/")
    |> Enum.map_join("/", &uri_encode/1)
  end

  defp uri_encode(value) do
    URI.encode(to_string(value), &URI.char_unreserved?/1)
  end

  defp maybe_put_param(map, _key, nil), do: map
  defp maybe_put_param(map, key, value), do: Map.put(map, key, value)

  defp maybe_put_disposition(map, nil), do: map

  defp maybe_put_disposition(map, filename) do
    Map.put(map, "response-content-disposition", ~s(attachment; filename="#{filename}"))
  end

  defp maybe_put_upload_length(headers, byte_size)
       when is_integer(byte_size) and byte_size >= 0 do
    headers ++ [{"x-upload-content-length", Integer.to_string(byte_size)}]
  end

  defp maybe_put_upload_length(headers, _), do: headers

  defp get_header(headers, target_name) do
    Enum.find_value(headers, fn {name, value} ->
      if String.downcase(to_string(name)) == target_name do
        to_string(value)
      end
    end)
  end

  defp stringify_keys(map) do
    Map.new(map, fn {k, v} -> {to_string(k), v} end)
  end
end
