defmodule SparkEx.MergeIntoWriter do
  @moduledoc """
  Builder for MERGE INTO table commands.

  Constructed via `SparkEx.DataFrame.merge_into/2`, configured with match
  actions, and executed with `merge/1`.

  ## Examples

      import SparkEx.Functions, only: [col: 1]

      df
      |> DataFrame.merge_into("target_table")
      |> MergeIntoWriter.on(col("source.id") |> Column.eq(col("target.id")))
      |> MergeIntoWriter.when_matched_update_all()
      |> MergeIntoWriter.when_not_matched_insert_all()
      |> MergeIntoWriter.merge()
  """

  alias SparkEx.{Column, DataFrame}

  defstruct [
    :source_df,
    :target_table,
    :condition,
    match_actions: [],
    not_matched_actions: [],
    not_matched_by_source_actions: [],
    schema_evolution: false
  ]

  @type action ::
          {:delete | :insert | :insert_star | :update | :update_star, Column.expr() | nil,
           [{Column.expr(), Column.expr()}]}

  @type t :: %__MODULE__{
          source_df: DataFrame.t(),
          target_table: String.t(),
          condition: Column.expr() | nil,
          match_actions: [action()],
          not_matched_actions: [action()],
          not_matched_by_source_actions: [action()],
          schema_evolution: boolean()
        }

  @doc false
  def new(%DataFrame{} = df, table_name) when is_binary(table_name) do
    %__MODULE__{source_df: df, target_table: table_name}
  end

  @doc """
  Sets the merge condition (ON clause).
  """
  @spec on(t(), Column.t()) :: t()
  def on(%__MODULE__{} = m, %Column{} = condition) do
    %{m | condition: condition.expr}
  end

  # ── whenMatched actions ──

  @doc "Deletes matched rows. Optional condition filters which matched rows to delete."
  @spec when_matched_delete(t(), Column.t() | nil) :: t()
  def when_matched_delete(%__MODULE__{} = m, condition \\ nil) do
    action = {:delete, normalize_condition(condition), []}
    %{m | match_actions: [action | m.match_actions]}
  end

  @doc "Updates all columns of matched rows."
  @spec when_matched_update_all(t(), Column.t() | nil) :: t()
  def when_matched_update_all(%__MODULE__{} = m, condition \\ nil) do
    action = {:update_star, normalize_condition(condition), []}
    %{m | match_actions: [action | m.match_actions]}
  end

  @doc """
  Updates specific columns of matched rows.

  `assignments` is a map of `%{"target_col" => Column.t()}`.
  """
  @spec when_matched_update(t(), %{String.t() => Column.t()}, Column.t() | nil) :: t()
  def when_matched_update(%__MODULE__{} = m, assignments, condition \\ nil)
      when is_map(assignments) do
    action = {:update, normalize_condition(condition), normalize_assignments(assignments)}
    %{m | match_actions: [action | m.match_actions]}
  end

  # ── whenNotMatched actions ──

  @doc "Inserts all columns for non-matched source rows."
  @spec when_not_matched_insert_all(t(), Column.t() | nil) :: t()
  def when_not_matched_insert_all(%__MODULE__{} = m, condition \\ nil) do
    action = {:insert_star, normalize_condition(condition), []}
    %{m | not_matched_actions: [action | m.not_matched_actions]}
  end

  @doc """
  Inserts specific columns for non-matched source rows.

  `assignments` is a map of `%{"target_col" => Column.t()}`.
  """
  @spec when_not_matched_insert(t(), %{String.t() => Column.t()}, Column.t() | nil) :: t()
  def when_not_matched_insert(%__MODULE__{} = m, assignments, condition \\ nil)
      when is_map(assignments) do
    action = {:insert, normalize_condition(condition), normalize_assignments(assignments)}
    %{m | not_matched_actions: [action | m.not_matched_actions]}
  end

  # ── whenNotMatchedBySource actions ──

  @doc "Deletes target rows not matched by source."
  @spec when_not_matched_by_source_delete(t(), Column.t() | nil) :: t()
  def when_not_matched_by_source_delete(%__MODULE__{} = m, condition \\ nil) do
    action = {:delete, normalize_condition(condition), []}
    %{m | not_matched_by_source_actions: [action | m.not_matched_by_source_actions]}
  end

  @doc "Updates all columns of target rows not matched by source."
  @spec when_not_matched_by_source_update_all(t(), Column.t() | nil) :: t()
  def when_not_matched_by_source_update_all(%__MODULE__{} = m, condition \\ nil) do
    action = {:update_star, normalize_condition(condition), []}
    %{m | not_matched_by_source_actions: [action | m.not_matched_by_source_actions]}
  end

  @doc """
  Updates specific columns of target rows not matched by source.

  `assignments` is a map of `%{"target_col" => Column.t()}`.
  """
  @spec when_not_matched_by_source_update(t(), %{String.t() => Column.t()}, Column.t() | nil) ::
          t()
  def when_not_matched_by_source_update(%__MODULE__{} = m, assignments, condition \\ nil)
      when is_map(assignments) do
    action = {:update, normalize_condition(condition), normalize_assignments(assignments)}
    %{m | not_matched_by_source_actions: [action | m.not_matched_by_source_actions]}
  end

  @doc "Enables schema evolution for the merge operation."
  @spec with_schema_evolution(t()) :: t()
  def with_schema_evolution(%__MODULE__{} = m) do
    %{m | schema_evolution: true}
  end

  @doc """
  Executes the MERGE INTO command.

  Returns `:ok` or `{:error, reason}`.
  """
  @spec merge(t()) :: :ok | {:error, term()}
  def merge(%__MODULE__{condition: nil}) do
    raise ArgumentError, "merge condition (ON clause) is required"
  end

  def merge(%__MODULE__{} = m) do
    command =
      {:merge_into_table, m.source_df.plan, m.target_table, m.condition, Enum.reverse(m.match_actions),
       Enum.reverse(m.not_matched_actions), Enum.reverse(m.not_matched_by_source_actions),
       m.schema_evolution}

    SparkEx.Session.execute_command(m.source_df.session, command)
  end

  # ── Private helpers ──

  defp normalize_condition(nil), do: nil
  defp normalize_condition(%Column{expr: e}), do: e

  defp normalize_assignments(assignments) when is_map(assignments) do
    Enum.map(assignments, fn
      {col_name, %Column{expr: value_expr}} when is_binary(col_name) ->
        {{:col, col_name}, value_expr}

      {col_name, _} when not is_binary(col_name) ->
        raise ArgumentError,
              "merge assignment keys must be strings, got: #{inspect(col_name)}"
    end)
  end
end
