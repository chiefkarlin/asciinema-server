defmodule Asciinema.FileStore.GCS do
  use Asciinema.Config
  use Asciinema.FileStore
  import Phoenix.Controller, only: [redirect: 2]
  import Plug.Conn

  @impl true
  def url(path) do
    url(path, [])
  end

  def url(path, query_params) do
    conn = gcs_conn()

    case GoogleApi.Storage.V1.Api.Objects.storage_objects_get(conn, bucket(), base_path() <> path, query_params) do
      {:ok, object} ->
        object.media_link
      {:error, _} ->
        nil
    end
  end

  @impl true
  def put_file(dst_path, src_local_path, content_type) do
    conn = gcs_conn()
    stream = File.stream!(src_local_path, [], 64 * 1024)

    case GoogleApi.Storage.V1.Api.Objects.storage_objects_insert(
           conn,
           bucket(),
           %GoogleApi.Storage.V1.Model.Object{
             name: base_path() <> dst_path,
             contentType: content_type
           },
           upload_type: "resumable",
           body: stream
         ) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def move_file(from_path, to_path) do
    conn = gcs_conn()

    case GoogleApi.Storage.V1.Api.Objects.storage_objects_copy(
           conn,
           bucket(),
           base_path() <> from_path,
           bucket(),
           base_path() <> to_path
         ) do
      {:ok, _} -> delete_file(from_path)
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def serve_file(conn, path, filename) do
    do_serve_file(conn, path, filename, config(:proxy_path_prefix))
  end

  defp do_serve_file(conn, path, filename, nil) do
    redirect(conn, external: url(path, gcs_response_params(filename)))
  end

  defp do_serve_file(conn, path, filename, proxy_path_prefix) do
    conn
    |> put_resp_header("x-accel-redirect", "#{proxy_path_prefix}#{path}")
    |> put_resp_header("redirect-uri", url(path))
    |> put_content_disposition(filename)
    |> send_resp(200, "")
  end

  defp gcs_response_params(nil), do: []

  defp gcs_response_params(filename) do
    ["response-content-disposition": "attachment; filename=#{filename}"]
  end

  defp put_content_disposition(conn, nil), do: conn

  defp put_content_disposition(conn, filename) do
    put_resp_header(conn, "content-disposition", "attachment; filename=#{filename}")
  end

  @impl true
  def open_file(path, function \\ nil) do
    conn = gcs_conn()

    case GoogleApi.Storage.V1.Api.Objects.storage_objects_get(conn, bucket(), base_path() <> path, alt: "media") do
      {:ok, %{body: body}} ->
        {:ok, tmp_path} = Briefly.create()
        File.write!(tmp_path, body)

        if function do
          File.open(tmp_path, [:binary, :read], function)
        else
          File.open(tmp_path, [:binary, :read])
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def delete_file(path) do
    conn = gcs_conn()

    case GoogleApi.Storage.V1.Api.Objects.storage_objects_delete(conn, bucket(), base_path() <> path) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp bucket, do: config(:bucket)

  defp base_path, do: config(:path)

  defp gcs_conn, do: Asciinema.GCS.gcs_conn()
end
