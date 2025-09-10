defmodule Asciinema.FileStore.GCSTest do
  use Asciinema.DataCase, async: true

  alias Asciinema.FileStore.GCS
  alias Asciinema.Recordings

  @bucket System.get_env("GCS_BUCKET")

  describe "put_file/3 and url/1" do
    test "uploads a file and returns a signed URL" do
      temp_path = System.tmp_dir() <> "/test.txt"
      File.write!(temp_path, "hello world")

      assert :ok == GCS.put_file("test.txt", temp_path, "text/plain")
      assert GCS.url("test.txt") =~ "https://storage.googleapis.com/"
    end
  end

  describe "create_asciicast/3 with GCS URI" do
    test "creates an asciicast from a GCS URI" do
      user = insert(:user)
      gcs_uri = "gs://#{@bucket}/welcome.cast"

      {:ok, asciicast} = Recordings.create_asciicast(user, gcs_uri)

      assert asciicast.user_id == user.id
      assert asciicast.version == 2
    end
  end
end
