defmodule AsciinemaWeb.Api.RecordingController do
  use AsciinemaWeb, :controller
  use Asciinema.Config
  alias Asciinema.{Accounts, FileStore, Recordings}
  alias AsciinemaWeb.Api.AuthError
  alias AsciinemaWeb.Plug.Authz

  plug :assign_install_id
  plug :assign_cli
  plug :enforce_upload_limit when action in [:create, :initiate_upload, :complete_upload]
  plug :require_registered_cli when action in [:update, :delete]
  plug :load_asciicast when action in [:update, :delete]

  def initiate_upload(conn, params) do
    cli = conn.assigns.cli

    if FileStore.direct_upload_supported?() do
      object_key = "tmp/direct-uploads/#{Ecto.UUID.generate()}.cast"
      content_type = params["content_type"] || "application/x-asciicast"

      opts = [
        content_type: content_type,
        byte_size: params["byte_size"]
      ]

      case FileStore.initiate_direct_upload(object_key, opts) do
        {:ok, upload_spec} ->
          upload_id =
            Recordings.generate_direct_upload_token(%{
              object_key: object_key,
              cli_id: cli.id,
              user_id: cli.user.id,
              filename: params["filename"],
              byte_size: params["byte_size"]
            })

          payload = %{
            mode: "direct",
            upload_id: upload_id,
            upload_token: upload_id,
            object_key: object_key,
            upload_url: upload_spec.upload_url,
            upload_method: Map.get(upload_spec, :upload_method, "PUT"),
            upload_type: Map.get(upload_spec, :upload_type, "resumable"),
            headers:
              Map.get(upload_spec, :headers, %{"Content-Type" => "application/x-asciicast"}),
            complete_url: url(~p"/api/v1/recordings/uploads/complete")
          }

          conn
          |> put_status(:ok)
          |> json(payload)

        {:error, _reason} ->
          conn
          |> put_status(:unprocessable_entity)
          |> render(:error, reason: :direct_upload_unsupported)
      end
    else
      conn
      |> put_status(:unprocessable_entity)
      |> render(:error, reason: :direct_upload_unsupported)
    end
  end

  def complete_upload(conn, params) do
    cli = conn.assigns.cli
    user_agent = get_user_agent(conn)
    path_upload_id = conn.path_params["upload_id"]
    body_token = params["upload_token"] || params["upload_id"]

    with {:ok, token_data} <-
           verify_upload_token(body_token, path_upload_id, cli, params["object_key"]) do
      params =
        case params["filename"] do
          f when is_binary(f) and f != "" -> params
          _ -> Map.put(params, "filename", token_data[:filename] || "asciicast.cast")
        end

      case Recordings.create_asciicast_from_store(
             cli.user,
             token_data.object_key,
             %{cli_id: cli.id, user_agent: user_agent},
             params
           ) do
        {:ok, asciicast} ->
          conn
          |> put_status(:created)
          |> put_resp_header("location", url(~p"/a/#{asciicast}"))
          |> render(:show, asciicast: asciicast)

        {:error, reason} when reason in [:enoent, :not_found] ->
          conn
          |> put_status(:unprocessable_entity)
          |> render(:error, reason: :upload_not_found)

        {:error, reason} ->
          _ = FileStore.delete_file(token_data.object_key)

          conn
          |> put_status(:unprocessable_entity)
          |> render(:error, reason: reason)
      end
    else
      {:error, :invalid_upload_token} ->
        conn
        |> put_status(:unprocessable_entity)
        |> render(:error, reason: :invalid_upload_token)
    end
  end

  defp verify_upload_token(token, path_upload_id, cli, param_object_key)
       when is_binary(token) do
    with {:ok, %{object_key: object_key, cli_id: cli_id, user_id: user_id} = data} <-
           Recordings.verify_direct_upload_token(token),
         true <- cli_id == cli.id and user_id == cli.user.id,
         true <- String.starts_with?(object_key, "tmp/direct-uploads/"),
         true <- param_object_key in [nil, object_key],
         true <- path_upload_id in [nil, token, data[:upload_id]] do
      {:ok, data}
    else
      _ -> {:error, :invalid_upload_token}
    end
  end

  defp verify_upload_token(_token, _path_upload_id, _cli, _param_object_key),
    do: {:error, :invalid_upload_token}

  def create(conn, %{"asciicast" => %Plug.Upload{}} = params),
    do: create(conn, Map.pop(params, "asciicast"))

  def create(conn, %{"file" => %Plug.Upload{}} = params),
    do: create(conn, Map.pop(params, "file"))

  def create(conn, {upload, params}) do
    cli = conn.assigns.cli
    user_agent = get_user_agent(conn)
    params = Map.put(params, "filename", upload.filename)

    case Recordings.create_asciicast(
           cli.user,
           upload.path,
           %{cli_id: cli.id, user_agent: user_agent},
           params
         ) do
      {:ok, asciicast} ->
        conn
        |> put_status(:created)
        |> put_resp_header("location", url(~p"/a/#{asciicast}"))
        |> render(:show, asciicast: asciicast)

      {:error, reason} ->
        conn
        |> put_status(:unprocessable_entity)
        |> render(:error, reason: reason)
    end
  end

  def update(conn, params) do
    asciicast = conn.assigns.asciicast

    case Recordings.update_asciicast(asciicast, params) do
      {:ok, asciicast} ->
        render(conn, :show, asciicast: asciicast)

      {:error, changeset} ->
        conn
        |> put_status(:unprocessable_entity)
        |> render(:error, reason: changeset)
    end
  end

  def delete(conn, _params) do
    asciicast = conn.assigns.asciicast
    {:ok, _} = Recordings.delete_asciicast(asciicast)

    send_resp(conn, :no_content, "")
  end

  defp get_user_agent(conn) do
    conn
    |> get_req_header("user-agent")
    |> List.first()
  end

  defp assign_install_id(conn, _opts) do
    case get_basic_auth(conn) do
      {username, install_id} ->
        conn
        |> assign(:install_id, install_id)
        |> assign(:username, username)

      _otherwise ->
        AuthError.render_error(conn, :missing)
    end
  end

  defp assign_cli(conn, _opts) do
    %{install_id: install_id, username: username} = conn.assigns

    with {:ok, cli} <- Accounts.get_or_create_upload_cli(username, install_id) do
      conn
      |> assign(:cli, cli)
      |> assign(:current_user, cli.user)
    else
      {:error, reason} ->
        AuthError.render_error(conn, cli_error_kind(reason))
    end
  end

  defp cli_error_kind(:token_invalid), do: :invalid
  defp cli_error_kind(:token_not_found), do: :no_account
  defp cli_error_kind(:cli_revoked), do: :revoked

  defp enforce_upload_limit(conn, _opts) do
    cli = conn.assigns.cli
    limit = Accounts.unregistered_upload_count_limit()

    if limit && !Accounts.cli_registered?(cli) &&
         Recordings.count_user_asciicasts(cli.user) >= limit do
      conn
      |> put_status(:forbidden)
      |> render(:error,
        reason: :upload_limit,
        message:
          "Unregistered upload limit reached (#{limit} #{if limit == 1, do: "recording", else: "recordings"})"
      )
      |> halt()
    else
      conn
    end
  end

  defp require_registered_cli(conn, _opts) do
    if Accounts.cli_registered?(conn.assigns.cli) do
      conn
    else
      AuthError.render_error(conn, :no_account)
    end
  end

  defp load_asciicast(conn, _) do
    id = String.trim(conn.params["id"])

    with asciicast when not is_nil(asciicast) <-
           Recordings.lookup_asciicast(id, allow_non_public_id: true),
         true <- Authz.authorized?(conn, asciicast) do
      assign(conn, :asciicast, asciicast)
    else
      _ ->
        conn
        |> put_status(:not_found)
        |> render(:error, reason: :not_found)
        |> halt()
    end
  end
end
