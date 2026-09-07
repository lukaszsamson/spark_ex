defmodule SparkEx.Artifacts do
  @moduledoc """
  Convenience helpers for uploading local artifacts to a Spark Connect session.

  These helpers prefix artifact names with the appropriate category and
  upload via `AddArtifacts`. File contents are streamed from disk in
  chunks (peak memory ~ chunk size, not file size), so large jars and
  archives can be sent without loading them whole.

  ## Hash-fragment aliases

  Path strings of the form `"/local/path/foo.jar#alias"` are accepted
  for `add_files`, `add_jars`, `add_archives`, and `add_pyfiles`. The
  fragment after `#` is used as the artifact basename instead of the
  filesystem name. This mirrors Spark's `addFile(path, recursive,
  alias)` / `addJar(path)#alias` semantics.
  """

  @type artifact_data :: binary() | {:file, Path.t(), non_neg_integer()}
  @type artifact_entry :: {String.t(), artifact_data()}

  @archive_extensions ~w(.zip .jar .tar.gz .tgz .tar)
  @jar_extensions ~w(.jar)
  @pyfile_extensions ~w(.py .zip .egg .jar)

  # Matches an absolute Windows drive path such as `C:\foo\bar.jar` or
  # `C:/foo/bar.jar`. A single-letter prefix followed by `:` and a path
  # separator is a drive letter, not a URI scheme (see SPARK-55071 in
  # upstream PySpark's `artifact.py:_parse_artifacts`, which hit exactly
  # this: `urlparse("C:\\foo")` mis-parses the drive letter as scheme `c`).
  @windows_drive_path ~r/^[A-Za-z]:[\\\/]/

  @doc """
  Classifies a raw artifact path/URI string into the local filesystem path
  that should be used to read the file's contents.

  This is a pure string transformation — it does not touch the filesystem
  — so it can be unit-tested with Windows-shaped inputs from any host OS.

  Handles, in order:

    * an absolute Windows drive path (`C:\\foo\\bar.jar`, `C:/foo/bar.jar`)
      — detected *before* URI parsing so the drive letter is never mistaken
      for a URI scheme; returned unchanged.
    * a UNC path (`\\\\host\\share\\file.jar`) — returned unchanged.
    * a `file:` URI (`file:///tmp/x.jar`, `file:/tmp/x.jar`,
      `file:///C:/x.jar`, `file:/C:/x.jar`) — percent-decoded to the
      underlying local path, with the extra leading `/` that URI syntax
      puts before a drive letter (`/C:/x.jar`) stripped off.
    * anything else (an ordinary POSIX path, or a remote URI such as
      `hdfs://...` or `s3://...`) — returned unchanged. spark_ex does not
      resolve remote artifacts locally, so these behave exactly as before
      this function existed: passed through as-is and left to fail (or
      succeed, for a real POSIX path) via `File.stat/1` downstream.
  """
  @spec classify_local_path(String.t()) :: String.t()
  def classify_local_path(raw) when is_binary(raw) do
    cond do
      windows_drive_path?(raw) -> raw
      unc_path?(raw) -> raw
      true -> decode_file_uri(raw) || raw
    end
  end

  defp windows_drive_path?(raw), do: Regex.match?(@windows_drive_path, raw)

  defp unc_path?(<<?\\, ?\\, _rest::binary>>), do: true
  defp unc_path?(_raw), do: false

  # Returns the decoded local path for a `file:` URI, or `nil` for anything
  # else (including other URI schemes, which are left for the caller to
  # pass through unchanged).
  defp decode_file_uri(raw) do
    case URI.parse(raw) do
      %URI{scheme: "file", path: path} when is_binary(path) ->
        path
        |> URI.decode()
        |> strip_uri_drive_slash()

      _uri ->
        nil
    end
  end

  # `file:///C:/foo` parses to URI path `/C:/foo` — URI syntax always
  # anchors the path with a leading `/`, which needs stripping when what
  # follows is actually a Windows drive letter.
  defp strip_uri_drive_slash(<<?/, rest::binary>> = path) do
    if windows_drive_path?(rest), do: rest, else: path
  end

  defp strip_uri_drive_slash(path), do: path

  @doc """
  Prepares artifact entries by validating local paths and prefixing names.

  Returns lazy `{name, {:file, path, size}}` entries — the file is
  read in chunks at upload time, not here.
  """
  @spec prepare(String.t() | [String.t()], String.t()) ::
          {:ok, [artifact_entry()]} | {:error, term()}
  def prepare(paths, prefix) when is_binary(prefix) do
    prepare(paths, prefix, [])
  end

  @doc false
  @spec prepare(String.t() | [String.t()], String.t(), keyword()) ::
          {:ok, [artifact_entry()]} | {:error, term()}
  def prepare(paths, prefix, opts) when is_binary(prefix) and is_list(opts) do
    normalized_prefix = normalize_prefix(prefix)
    allowed_extensions = Keyword.get(opts, :extensions, nil)
    archive? = Keyword.get(opts, :archive, false)
    paths = normalize_paths(paths)

    with :ok <- validate_extensions(paths, allowed_extensions),
         {:ok, entries} <- stat_paths(paths, archive?) do
      artifacts =
        Enum.map(entries, fn {real_path, alias_name, size} ->
          {normalized_prefix <> alias_name, {:file, real_path, size}}
        end)

      names = Enum.map(artifacts, &elem(&1, 0))
      dupes = names -- Enum.uniq(names)

      if dupes != [] do
        raise ArgumentError,
              "duplicate artifact names after path normalization: #{inspect(Enum.uniq(dupes))}"
      end

      {:ok, artifacts}
    end
  end

  @doc """
  Uploads JAR files from local paths.

  Each path must end in `.jar`. Hash-fragment aliases are honored:
  `"foo.jar#bar.jar"` is uploaded as `bar.jar`.
  """
  @spec add_jars(GenServer.server(), String.t() | [String.t()]) ::
          {:ok, [{String.t(), boolean()}]} | {:error, term()}
  def add_jars(session, paths) do
    add_with_prefix(session, paths, "jars/", extensions: @jar_extensions)
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

  Each path must end in one of `.zip`, `.jar`, `.tar.gz`, `.tgz`, or
  `.tar`.
  """
  @spec add_archives(GenServer.server(), String.t() | [String.t()]) ::
          {:ok, [{String.t(), boolean()}]} | {:error, term()}
  def add_archives(session, paths) do
    add_with_prefix(session, paths, "archives/",
      extensions: @archive_extensions,
      archive: true
    )
  end

  @doc """
  Uploads Python files from local paths.

  Mirrors PySpark's `addPyFile`/`addPyFiles`: each path must end in one
  of `.py`, `.zip`, `.egg`, or `.jar`.
  """
  @spec add_pyfiles(GenServer.server(), String.t() | [String.t()]) ::
          {:ok, [{String.t(), boolean()}]} | {:error, term()}
  def add_pyfiles(session, paths) do
    paths = normalize_paths(paths)

    case validate_extensions(paths, @pyfile_extensions) do
      :ok -> add_with_prefix(session, paths, "pyfiles/")
      {:error, _} = error -> error
    end
  end

  defp add_with_prefix(session, paths, prefix, opts \\ []) do
    with {:ok, artifacts} <- prepare(paths, prefix, opts) do
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

  defp validate_extensions(_paths, nil), do: :ok

  defp validate_extensions(paths, allowed) when is_list(allowed) do
    bad =
      Enum.reject(paths, fn raw ->
        {real, _alias_name} = split_fragment(raw)
        # Mirror PySpark: archive/jar/pyfile extension is checked against
        # the *real* file's basename, not the alias fragment. Classify
        # first so a `file:` URI or Windows drive path's extension is
        # checked against its actual filename, not raw URI/UNC syntax.
        real |> classify_local_path() |> ext_match?(allowed)
      end)

    case bad do
      [] -> :ok
      [_ | _] -> {:error, {:invalid_artifact_extension, bad, allowed}}
    end
  end

  defp ext_match?(path, allowed) do
    lower = String.downcase(path)
    Enum.any?(allowed, &String.ends_with?(lower, &1))
  end

  defp normalize_prefix(prefix) do
    prefix
    |> String.trim_trailing("/")
    |> Kernel.<>("/")
  end

  # Splits a `path#alias` string into `{real_path, alias_or_nil}`. The
  # fragment after the *first* `#` is treated as the alias (any further
  # `#` characters are kept verbatim in the alias). Paths without `#`
  # return `{path, nil}`.
  defp split_fragment(raw) when is_binary(raw) do
    case String.split(raw, "#", parts: 2) do
      [path] -> {path, nil}
      [path, ""] -> {path, nil}
      [path, alias_name] -> {path, alias_name}
    end
  end

  defp stat_paths(paths, archive?) do
    Enum.reduce_while(paths, {:ok, []}, fn raw, {:ok, acc} ->
      {raw_path, fragment} = split_fragment(raw)
      real_path = classify_local_path(raw_path)

      case File.stat(real_path) do
        {:ok, %File.Stat{type: :regular, size: size}} ->
          basename = Path.basename(real_path)

          alias_name =
            cond do
              fragment == nil -> basename
              # PySpark archives keep the basename as part of the alias so
              # the server can pick the right unpacker by file extension:
              # `name = f"{basename}#{fragment}"`. See pyspark
              # connect/client/artifact.py:_parse_artifacts.
              archive? -> basename <> "#" <> fragment
              true -> fragment
            end

          {:cont, {:ok, [{real_path, alias_name, size} | acc]}}

        {:ok, %File.Stat{type: type}} ->
          {:halt, {:error, {:not_a_regular_file, real_path, type}}}

        {:error, reason} ->
          {:halt, {:error, {:file_stat_error, real_path, reason}}}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      {:error, _} = error -> error
    end
  end
end
