defmodule AsciinemaWeb.Api.RecordingControllerGCSTest do
  use AsciinemaWeb.ConnCase, async: true

  alias Asciinema.Recordings

  @bucket System.get_env("GCS_BUCKET")

  test "POST /api/asciicasts with gcs_uri", %{conn: conn} do
    user = insert(:user)
    gcs_uri = "gs://#{@bucket}/welcome.cast"

    conn =
      conn
      |> put_req_header("authorization", "Basic #{Base.encode64(user.username <> ":" <> user.install_id)}")
      |> post(~p"/api/asciicasts", %{gcs_uri: gcs_uri})

    assert json_response(conn, 201)
    assert %{"id" => id} = json_response(conn, 201)
    assert asciicast = Recordings.get_asciicast(id)
    assert asciicast.user_id == user.id
  end
end
