defmodule SparkEx.Artifacts do
  @moduledoc """
  Convenience helpers for uploading local artifacts to a Spark Connect session.

  These helpers read local files and upload them via `AddArtifacts`,
  prefixing names with the appropriate category.
  """

  @type artifact_entry :: {String.t(), binary()}

  @doc """
  Prepares artifact entries by reading local paths and prefixing names.
  """
  @spec prepare(String.t() | [String.t()], String.t()) ::
          {:ok, [artifact_entry()]} | {:error, term()}
  def prepare(paths, prefix) when is_binary(prefix) do
    normalized_prefix = normalize_prefix(prefix)

    with {:ok, entries} <- read_paths(normalize_paths(paths)) do
      artifacts =
        Enum.map(entries, fn {path, data} ->
          {normalized_prefix <> Path.basename(path), data}
        end)

      {:ok, artifacts}
    end
  end

  @doc """
  Uploads JAR files from local paths.
  """
  @spec add_jars(GenServer.server(), String.t() | [String.t()]) ::
          {:ok, [{String.t(), boolean()}]} | {:error, term()}
  def add_jars(session, paths) do
    add_with_prefix(session, paths, "jars/")
  end

  @doc """
  Uploads file artifacts from local paths.
  """
  @spec add_files(GenServer.server(), String.t() | [String.t()]) ::
          {:ok, [{String.t(), boolean()}]} | {:error, term()}
  def add_files(session, paths) do
    add_with_prefix(session, paths, "files/")
  end

  @doc """
  Uploads archive artifacts from local paths.
  """
  @spec add_archives(GenServer.server(), String.t() | [String.t()]) ::
          {:ok, [{String.t(), boolean()}]} | {:error, term()}
  def add_archives(session, paths) do
    add_with_prefix(session, paths, "archives/")
  end

  @doc """
  Uploads Python files from local paths.
  """
  @spec add_pyfiles(GenServer.server(), String.t() | [String.t()]) ::
          {:ok, [{String.t(), boolean()}]} | {:error, term()}
  def add_pyfiles(session, paths) do
    add_with_prefix(session, paths, "pyfiles/")
  end

  defp add_with_prefix(session, paths, prefix) do
    with {:ok, artifacts} <- prepare(paths, prefix) do
      SparkEx.Session.add_artifacts(session, artifacts)
    end
  end

  defp normalize_paths(path) when is_binary(path), do: [path]

  defp normalize_paths(paths) when is_list(paths) do
    unless Enum.all?(paths, &is_binary/1) do
      raise ArgumentError, "expected paths to be a string or list of strings"
    end

    paths
  end

  defp normalize_paths(_paths) do
    raise ArgumentError, "expected paths to be a string or list of strings"
  end

  defp normalize_prefix(prefix) do
    prefix
    |> String.trim_trailing("/")
    |> Kernel.<>("/")
  end

  defp read_paths(paths) do
    Enum.reduce_while(paths, {:ok, []}, fn path, {:ok, acc} ->
      case File.read(path) do
        {:ok, data} -> {:cont, {:ok, [{path, data} | acc]}}
        {:error, reason} -> {:halt, {:error, {:file_read_error, path, reason}}}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      {:error, _} = error -> error
    end
  end
end
