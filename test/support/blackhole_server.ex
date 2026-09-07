defmodule SparkEx.Test.BlackholeServer do
  @moduledoc false

  # A TCP listener that accepts connections and then never answers: bytes are
  # read and discarded, nothing is ever written back. Used to exercise the
  # "server is reachable but unresponsive" path (session stop / shutdown must
  # stay bounded) without a real Spark Connect server.

  @doc """
  Starts a black-hole listener and returns its port.

  The listener (and its acceptor process) are linked to the calling process,
  so they go away with the test.
  """
  @spec start_link() :: {:ok, :inet.port_number(), pid()}
  def start_link do
    {:ok, listen_socket} =
      :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false, reuseaddr: true, backlog: 8])

    {:ok, port} = :inet.port(listen_socket)

    acceptor =
      spawn_link(fn ->
        receive do
          :owned -> accept_loop(listen_socket)
        end
      end)

    :ok = :gen_tcp.controlling_process(listen_socket, acceptor)
    send(acceptor, :owned)

    {:ok, port, acceptor}
  end

  defp accept_loop(listen_socket) do
    case :gen_tcp.accept(listen_socket) do
      {:ok, socket} ->
        # Keep the socket open (and owned by a process that never replies)
        # so the client sees an established connection that never answers.
        spawn(fn -> drain(socket) end)
        accept_loop(listen_socket)

      {:error, _reason} ->
        :ok
    end
  end

  defp drain(socket) do
    case :gen_tcp.recv(socket, 0, 60_000) do
      {:ok, _data} -> drain(socket)
      {:error, _reason} -> :gen_tcp.close(socket)
    end
  end

  @doc """
  Returns a TCP port number that nothing is listening on.
  """
  @spec closed_port() :: :inet.port_number()
  def closed_port do
    {:ok, socket} = :gen_tcp.listen(0, ip: {127, 0, 0, 1}, active: false)
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end
end
