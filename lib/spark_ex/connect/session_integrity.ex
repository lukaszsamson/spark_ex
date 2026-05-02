defmodule SparkEx.Connect.SessionIntegrity do
  @moduledoc false

  # Spark Connect responses carry both `session_id` (the client-issued
  # session id) and `server_side_session_id` (a server-rotated id pinned
  # on the first non-empty value). This module enforces the two
  # invariants:
  #
  #   * every response's `session_id` matches the client's session id; a
  #     mismatch means the response is not ours and must be rejected.
  #   * the first non-empty `server_side_session_id` is pinned; a later
  #     change indicates the server rotated the session and the client
  #     must close.
  #
  # The functions here return `:ok` or `{:error, reason}`; callers are
  # responsible for translating reasons into errors and closing state.

  @spec validate_session_id(struct() | map(), SparkEx.Session.t()) ::
          :ok | {:error, {:session_id_mismatch, %{expected: String.t(), got: String.t()}}}
  def validate_session_id(%{session_id: nil}, _session), do: :ok
  def validate_session_id(%{session_id: ""}, _session), do: :ok

  def validate_session_id(%{session_id: response_id}, %{session_id: client_id})
      when is_binary(response_id) and is_binary(client_id) do
    if response_id == client_id do
      :ok
    else
      {:error, {:session_id_mismatch, %{expected: client_id, got: response_id}}}
    end
  end

  def validate_session_id(_response, _session), do: :ok

  @spec validate_server_session_id(
          String.t() | nil,
          String.t() | nil
        ) ::
          {:ok, String.t() | nil}
          | {:error, {:server_session_changed, %{pinned: String.t(), got: String.t()}}}
  def validate_server_session_id(nil, current), do: {:ok, current}
  def validate_server_session_id("", current), do: {:ok, current}

  def validate_server_session_id(new_id, nil) when is_binary(new_id), do: {:ok, new_id}

  def validate_server_session_id(new_id, current)
      when is_binary(new_id) and is_binary(current) do
    if new_id == current do
      {:ok, current}
    else
      {:error, {:server_session_changed, %{pinned: current, got: new_id}}}
    end
  end

  @doc """
  Validate a single response's session ids against the session state.

  Returns `:ok` together with the server-session id that should be pinned
  (which equals the existing one if no rotation happened), or an error
  describing the integrity violation.
  """
  @spec validate_response(struct() | map(), SparkEx.Session.t()) ::
          {:ok, String.t() | nil}
          | {:error, {:session_id_mismatch, %{expected: String.t(), got: String.t()}}}
          | {:error, {:server_session_changed, %{pinned: String.t(), got: String.t()}}}
  def validate_response(response, session) do
    with :ok <- validate_session_id(response, session) do
      response_server_id = Map.get(response, :server_side_session_id)
      validate_server_session_id(response_server_id, session.server_side_session_id)
    end
  end

  @doc """
  Returns `true` if the given gRPC/Spark error indicates the server has
  rotated the session (`INVALID_HANDLE.SESSION_CHANGED`), or the local
  integrity check observed a server-side session id rotation
  (`{:server_session_changed, _}`).

  Also unwraps `{:error, _}` tuples so callers can pass replies directly.
  """
  @spec session_changed_error?(term()) :: boolean()
  def session_changed_error?(%SparkEx.Error.Remote{
        error_class: "INVALID_HANDLE.SESSION_CHANGED"
      }),
      do: true

  def session_changed_error?(%SparkEx.Error.Remote{message: message}) when is_binary(message) do
    String.contains?(message, "INVALID_HANDLE.SESSION_CHANGED")
  end

  def session_changed_error?({:server_session_changed, _ctx}), do: true
  def session_changed_error?({:error, inner}), do: session_changed_error?(inner)

  def session_changed_error?(_), do: false
end
