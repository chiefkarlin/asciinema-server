defmodule Asciinema.GCS do
  use Asciinema.Config

  def gcs_conn do
    {:ok, conn} = Goth.Token.for_scope("https://www.googleapis.com/auth/cloud-platform")
    GoogleApi.Storage.V1.Connection.new(conn.token)
  end

  def download_gcs_object(bucket, object) do
    conn = gcs_conn()
    {:ok, tmp_path} = Briefly.create()
    {:ok, file} = File.open(tmp_path, [:write])

    stream_to = fn chunk, acc ->
      IO.binwrite(acc, chunk)
      {:ok, acc}
    end

    case GoogleApi.Storage.V1.Api.Objects.storage_objects_get(conn, bucket, object, alt: "media", stream_to: {stream_to, file}) do
      {:ok, _} ->
        File.close(file)
        {:ok, tmp_path}

      {:error, reason} ->
        File.close(file)
        {:error, reason}
    end
  end
end
