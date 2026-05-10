defmodule SparkEx.Test.ParitySmoke do
  @moduledoc """
  PySpark parity smoke helpers.

  Pairs each fixture under `test/fixtures/parity_smoke/<name>.bin`
  (produced by `test/spark_server/parity_smoke.py`) with a zero-arg
  Elixir builder that returns the equivalent SparkEx plan term.

  Both sides are encoded to `Spark.Connect.Plan`, normalized (plan_ids
  stripped, lambda variable names canonicalized), and structurally
  compared.

  ## Why structural, not byte-equal

  PySpark assigns `plan_id` at relation-construction time; SparkEx
  assigns it at encode time. The numbering schemes therefore differ
  even when the relation tree is identical. We strip `plan_id` from
  the comparison and diff what's left.
  """

  alias SparkEx.Connect.PlanEncoder
  alias Spark.Connect.{Expression, Plan}

  @fixture_root Path.expand("../fixtures/parity_smoke", __DIR__)

  @doc """
  Returns the on-disk fixture root.
  """
  def fixture_root, do: @fixture_root

  @doc """
  Loads the PySpark fixture for `name`. Returns `{:ok, plan}` or
  `{:error, :missing}` if the fixture has not been generated yet.
  """
  @spec load(String.t()) :: {:ok, Plan.t()} | {:error, :missing}
  def load(name) when is_binary(name) do
    path = Path.join(@fixture_root, name <> ".bin")

    case File.read(path) do
      {:ok, bytes} -> {:ok, Plan.decode(bytes)}
      {:error, :enoent} -> {:error, :missing}
    end
  end

  @doc """
  Encodes `plan_term` through SparkEx and returns the normalized `Plan`.
  """
  @spec encode_normalized(term()) :: Plan.t()
  def encode_normalized(plan_term) do
    {plan, _} = PlanEncoder.encode(plan_term, 0)
    normalize(plan)
  end

  @doc """
  Strips every struct field named `:plan_id` (covers `RelationCommon`,
  `UnresolvedAttribute`, `UnresolvedRegex`, `UnresolvedStar`,
  `SubqueryExpression`, and any future proto type carrying a plan_id)
  and canonicalizes lambda variable names so that two proto trees from
  different sources can be structurally diffed.
  """
  @spec normalize(Plan.t() | Relation.t() | term()) :: term()
  def normalize(value) do
    {value, _} = walk(value, %{})
    value
  end

  defp walk(%Expression.UnresolvedNamedLambdaVariable{name_parts: parts} = node, acc) do
    {parts, acc} = Enum.map_reduce(parts, acc, &rename_lambda/2)
    {%{node | name_parts: parts}, acc}
  end

  defp walk(%_{} = struct, acc), do: walk_struct(struct, acc)

  defp walk(list, acc) when is_list(list), do: Enum.map_reduce(list, acc, &walk/2)

  defp walk(map, acc) when is_map(map) do
    Enum.map_reduce(Map.to_list(map), acc, fn {k, v}, a ->
      {v2, a2} = walk(v, a)
      {{k, v2}, a2}
    end)
    |> then(fn {kvs, a} -> {Map.new(kvs), a} end)
  end

  defp walk({tag, val}, acc) when is_atom(tag) do
    {val, acc} = walk(val, acc)
    {{tag, val}, acc}
  end

  defp walk(other, acc), do: {other, acc}

  defp walk_struct(struct, acc) do
    {kvs, acc} =
      Map.from_struct(struct)
      |> Map.to_list()
      |> Enum.map_reduce(acc, fn
        {:plan_id, _v}, a ->
          {{:plan_id, 0}, a}

        {k, v}, a ->
          {v2, a2} = walk(v, a)
          {{k, v2}, a2}
      end)

    {struct(struct.__struct__, kvs), acc}
  end

  defp rename_lambda(name, acc) do
    case Regex.run(~r/^(.+?)_(\d+)$/, name) do
      [_, base, _suffix] ->
        case Map.fetch(acc, name) do
          {:ok, canonical} ->
            {canonical, acc}

          :error ->
            canonical = "__lvar#{map_size(acc)}_#{base}"
            {canonical, Map.put(acc, name, canonical)}
        end

      _ ->
        {name, acc}
    end
  end
end
