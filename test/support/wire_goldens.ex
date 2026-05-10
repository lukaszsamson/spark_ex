defmodule SparkEx.Test.WireGoldens do
  @moduledoc """
  Wire-format golden helpers for `Spark.Connect.Plan` proto bytes.

  Encodes an internal SparkEx plan term through `SparkEx.Connect.PlanEncoder`,
  serializes the resulting `Spark.Connect.Plan` to its protobuf wire bytes, and
  compares them against a fixture file under `test/fixtures/wire_goldens/`.

  Plans contain non-deterministic fragments (lambda variable names use
  `:erlang.unique_integer/1`); `canonicalize/1` rewrites those before
  serialization so byte-equality is meaningful.

  ## Workflow

      # First time / after intentional plan-encoding change:
      UPDATE_GOLDENS=1 mix test test/unit/wire_goldens_smoke_test.exs

      # Subsequent runs assert byte-equality.
      mix test test/unit/wire_goldens_smoke_test.exs

  ## Streams that should add goldens (BUGS_PLAN_5 Stream A)

  Each relation listed in Stream A should have at least one chain encoded
  here so that fixes for plan-id remap don't silently change wire bytes.
  """

  alias SparkEx.Connect.PlanEncoder
  alias Spark.Connect.{Expression, Plan, Relation}

  @fixture_root Path.expand("../fixtures/wire_goldens", __DIR__)

  @doc """
  Encodes `plan_term` to canonical proto bytes (lambda var names rewritten
  to deterministic indices so the bytes are stable across runs).
  """
  @spec encode_canonical(term(), non_neg_integer()) :: {binary(), Plan.t()}
  def encode_canonical(plan_term, counter \\ 0) do
    {plan, _counter} = PlanEncoder.encode(plan_term, counter)
    plan = canonicalize(plan)
    {Plan.encode(plan) |> IO.iodata_to_binary(), plan}
  end

  @doc """
  Asserts the encoded bytes for `plan_term` match the golden fixture
  at `test/fixtures/wire_goldens/<name>.bin`.

  Set `UPDATE_GOLDENS=1` in the environment to (re)write the fixture
  instead of asserting.
  """
  defmacro assert_golden(name, plan_term) do
    quote bind_quoted: [name: name, plan_term: plan_term] do
      SparkEx.Test.WireGoldens.__assert_golden__(name, plan_term)
    end
  end

  @doc false
  @dialyzer {:nowarn_function, __assert_golden__: 2}
  def __assert_golden__(name, plan_term) do
    {bytes, plan} = encode_canonical(plan_term)
    path = fixture_path(name)

    cond do
      System.get_env("UPDATE_GOLDENS") in ["1", "true"] ->
        File.mkdir_p!(Path.dirname(path))
        File.write!(path, bytes)

        raise ExUnit.AssertionError,
          message:
            "golden updated: #{Path.relative_to_cwd(path)} (re-run without UPDATE_GOLDENS to verify)"

      not File.exists?(path) ->
        File.mkdir_p!(Path.dirname(path))
        File.write!(path, bytes)

        raise ExUnit.AssertionError,
          message:
            "golden created: #{Path.relative_to_cwd(path)} — review and commit, then re-run"

      true ->
        expected = File.read!(path)

        if bytes == expected do
          {:ok, plan}
        else
          decoded_expected = Plan.decode(expected)

          raise ExUnit.AssertionError,
            message: """
            wire bytes diverged from golden #{Path.relative_to_cwd(path)}
            expected (decoded):
            #{inspect(decoded_expected, pretty: true, limit: :infinity)}

            actual (decoded):
            #{inspect(plan, pretty: true, limit: :infinity)}
            """
        end
    end
  end

  @doc """
  Rewrites lambda variable names (`x_<unique>` form) to deterministic
  indices `__lvar0`, `__lvar1`, ...

  Walks the encoded `Plan`/`Relation`/`Expression` tree.
  """
  @spec canonicalize(Plan.t()) :: Plan.t()
  def canonicalize(%Plan{} = plan) do
    {plan, _} = walk(plan, %{})
    plan_ids = collect_plan_ids(plan, %{})
    rewrite_plan_ids(plan, plan_ids)
  end

  import Bitwise

  # Plan_ids are allocated from a global atomic counter starting at 2^31, so
  # encoded bytes vary between runs. Renumber stable ids (≥ 2^31) to
  # deterministic positions based on first-occurrence in a structural walk.
  # Counter-based ids (< 2^31) are stable per encoding pass and left alone.
  @plan_id_base 1_000_000
  @stable_threshold 1 <<< 31

  @plan_id_carriers [
    Spark.Connect.RelationCommon,
    Spark.Connect.Expression.UnresolvedAttribute,
    Spark.Connect.Expression.UnresolvedRegex,
    Spark.Connect.Expression.UnresolvedStar,
    Spark.Connect.SubqueryExpression
  ]

  defp collect_plan_ids(%mod{plan_id: id} = struct, acc)
       when mod in @plan_id_carriers and is_integer(id) and id >= @stable_threshold do
    acc = Map.put_new(acc, id, map_size(acc) + @plan_id_base)
    descend_collect(struct, acc)
  end

  defp collect_plan_ids(%_{} = struct, acc), do: descend_collect(struct, acc)

  defp collect_plan_ids(list, acc) when is_list(list),
    do: Enum.reduce(list, acc, &collect_plan_ids/2)

  defp collect_plan_ids(tuple, acc) when is_tuple(tuple),
    do: tuple |> Tuple.to_list() |> Enum.reduce(acc, &collect_plan_ids/2)

  defp collect_plan_ids(map, acc) when is_map(map),
    do: map |> Map.values() |> Enum.reduce(acc, &collect_plan_ids/2)

  defp collect_plan_ids(_, acc), do: acc

  defp descend_collect(%_{} = struct, acc) do
    struct |> Map.from_struct() |> Map.values() |> Enum.reduce(acc, &collect_plan_ids/2)
  end

  defp rewrite_plan_ids(%mod{plan_id: id} = struct, plan_ids)
       when mod in @plan_id_carriers and is_integer(id) and id >= @stable_threshold do
    rewritten = %{struct | plan_id: Map.get(plan_ids, id, id)}
    rewrite_struct_fields(rewritten, plan_ids)
  end

  defp rewrite_plan_ids(%_{} = struct, plan_ids), do: rewrite_struct_fields(struct, plan_ids)

  defp rewrite_plan_ids(list, plan_ids) when is_list(list),
    do: Enum.map(list, &rewrite_plan_ids(&1, plan_ids))

  defp rewrite_plan_ids(tuple, plan_ids) when is_tuple(tuple) do
    tuple |> Tuple.to_list() |> Enum.map(&rewrite_plan_ids(&1, plan_ids)) |> List.to_tuple()
  end

  defp rewrite_plan_ids(map, plan_ids) when is_map(map) do
    Map.new(map, fn {k, v} -> {k, rewrite_plan_ids(v, plan_ids)} end)
  end

  defp rewrite_plan_ids(other, _plan_ids), do: other

  defp rewrite_struct_fields(struct, plan_ids) do
    fields =
      struct
      |> Map.from_struct()
      |> Enum.map(fn {k, v} -> {k, rewrite_plan_ids(v, plan_ids)} end)

    struct(struct.__struct__, fields)
  end

  defp walk(%Plan{} = plan, acc), do: walk_struct(plan, acc)

  defp walk(%Relation{} = rel, acc) do
    walk_struct(rel, acc)
  end

  defp walk(%Expression.UnresolvedNamedLambdaVariable{name_parts: parts} = node, acc) do
    {parts, acc} = Enum.map_reduce(parts, acc, &rename_lambda/2)
    {%{node | name_parts: parts}, acc}
  end

  defp walk(%Expression.LambdaFunction{arguments: args} = node, acc) do
    {args, acc} = Enum.map_reduce(args, acc, &walk/2)
    {function, acc} = walk(node.function, acc)
    {%{node | arguments: args, function: function}, acc}
  end

  defp walk(%_{} = struct, acc), do: walk_struct(struct, acc)

  defp walk(list, acc) when is_list(list), do: Enum.map_reduce(list, acc, &walk/2)

  defp walk(map, acc) when is_map(map) and not is_struct(map) do
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
    fields =
      Map.from_struct(struct)
      |> Map.to_list()
      |> Enum.map_reduce(acc, fn {k, v}, a ->
        {v2, a2} = walk(v, a)
        {{k, v2}, a2}
      end)

    {kvs, acc} = fields
    {struct(struct.__struct__, kvs), acc}
  end

  defp rename_lambda(name, acc) do
    case Regex.run(~r/^(.+?)_(\d+)$/, name) do
      [_, base, _suffix] ->
        key = name

        case Map.fetch(acc, key) do
          {:ok, canonical} ->
            {canonical, acc}

          :error ->
            canonical = "__lvar#{map_size(acc)}_#{base}"
            {canonical, Map.put(acc, key, canonical)}
        end

      _ ->
        {name, acc}
    end
  end

  @doc false
  def fixture_path(name) when is_binary(name) do
    Path.join(@fixture_root, name <> ".bin")
  end

  @doc "Returns the on-disk fixture root (absolute path)."
  def fixture_root, do: @fixture_root
end
