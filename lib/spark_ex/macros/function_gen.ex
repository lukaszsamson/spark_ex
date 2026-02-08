defmodule SparkEx.Macros.FunctionGen do
  @moduledoc false

  # Generates Spark SQL function wrappers from a declarative registry at compile time.
  #
  # Each registry entry {name, spark_name, arity_spec, opts} becomes a public function
  # with @doc, @spec, and any alias functions.

  alias SparkEx.Column

  @doc false
  defmacro generate_functions do
    entries = SparkEx.Macros.FunctionRegistry.registry()

    validate_no_duplicates!(entries)

    funcs =
      for {name, spark_name, arity_spec, opts} <- entries do
        is_distinct = Keyword.get(opts, :is_distinct, false)
        doc = build_doc(name, spark_name, opts)
        aliases = Keyword.get(opts, :aliases, [])

        primary = generate_function(name, spark_name, arity_spec, is_distinct, doc)
        alias_fns = Enum.map(aliases, &generate_alias(&1, name, arity_spec))

        [primary | alias_fns]
      end

    List.flatten(funcs)
  end

  # --- Validation ---

  defp validate_no_duplicates!(entries) do
    all_names =
      Enum.flat_map(entries, fn {name, _spark, _arity, opts} ->
        [name | Keyword.get(opts, :aliases, [])]
      end)

    dupes = all_names -- Enum.uniq(all_names)

    if dupes != [] do
      raise CompileError,
        description: "duplicate function names in registry: #{inspect(Enum.uniq(dupes))}"
    end
  end

  # --- Doc generation ---

  defp build_doc(_name, spark_name, opts) do
    base = Keyword.get(opts, :doc, "Calls Spark SQL function `#{spark_name}`.")
    "#{base}\n\nSpark SQL function: `#{spark_name}`"
  end

  # --- Primary function generation ---

  defp generate_function(name, spark_name, :zero, is_distinct, doc) do
    quote do
      @doc unquote(doc)
      @spec unquote(name)() :: Column.t()
      def unquote(name)() do
        %Column{expr: {:fn, unquote(spark_name), [], unquote(is_distinct)}}
      end
    end
  end

  defp generate_function(name, spark_name, :one_col, is_distinct, doc) do
    quote do
      @doc unquote(doc)
      @spec unquote(name)(Column.t() | String.t()) :: Column.t()
      def unquote(name)(col) do
        %Column{expr: {:fn, unquote(spark_name), [to_expr(col)], unquote(is_distinct)}}
      end
    end
  end

  defp generate_function(name, spark_name, :two_col, is_distinct, doc) do
    quote do
      @doc unquote(doc)
      @spec unquote(name)(Column.t() | String.t(), Column.t() | String.t()) :: Column.t()
      def unquote(name)(col1, col2) do
        %Column{
          expr: {:fn, unquote(spark_name), [to_expr(col1), to_expr(col2)], unquote(is_distinct)}
        }
      end
    end
  end

  defp generate_function(name, spark_name, :three_col, is_distinct, doc) do
    quote do
      @doc unquote(doc)
      @spec unquote(name)(
              Column.t() | String.t(),
              Column.t() | String.t(),
              Column.t() | String.t()
            ) :: Column.t()
      def unquote(name)(col1, col2, col3) do
        %Column{
          expr:
            {:fn, unquote(spark_name), [to_expr(col1), to_expr(col2), to_expr(col3)],
             unquote(is_distinct)}
        }
      end
    end
  end

  defp generate_function(name, spark_name, :n_col, is_distinct, doc) do
    quote do
      @doc unquote(doc)
      @spec unquote(name)([Column.t() | String.t()]) :: Column.t()
      def unquote(name)(cols) when is_list(cols) do
        %Column{
          expr: {:fn, unquote(spark_name), Enum.map(cols, &to_expr/1), unquote(is_distinct)}
        }
      end
    end
  end

  defp generate_function(name, spark_name, {:lit, 1}, is_distinct, doc) do
    quote do
      @doc unquote(doc)
      @spec unquote(name)(term()) :: Column.t()
      def unquote(name)(arg1) do
        %Column{expr: {:fn, unquote(spark_name), [lit_expr(arg1)], unquote(is_distinct)}}
      end
    end
  end

  defp generate_function(name, spark_name, {:lit, 2}, is_distinct, doc) do
    quote do
      @doc unquote(doc)
      @spec unquote(name)(term(), term()) :: Column.t()
      def unquote(name)(arg1, arg2) do
        %Column{
          expr: {:fn, unquote(spark_name), [lit_expr(arg1), lit_expr(arg2)], unquote(is_distinct)}
        }
      end
    end
  end

  defp generate_function(name, spark_name, {:col_lit, 1}, is_distinct, doc) do
    quote do
      @doc unquote(doc)
      @spec unquote(name)(Column.t() | String.t(), term()) :: Column.t()
      def unquote(name)(col, arg1) do
        %Column{
          expr: {:fn, unquote(spark_name), [to_expr(col), lit_expr(arg1)], unquote(is_distinct)}
        }
      end
    end
  end

  defp generate_function(name, spark_name, {:col_lit, 2}, is_distinct, doc) do
    quote do
      @doc unquote(doc)
      @spec unquote(name)(Column.t() | String.t(), term(), term()) :: Column.t()
      def unquote(name)(col, arg1, arg2) do
        %Column{
          expr:
            {:fn, unquote(spark_name), [to_expr(col), lit_expr(arg1), lit_expr(arg2)],
             unquote(is_distinct)}
        }
      end
    end
  end

  defp generate_function(name, spark_name, {:col_lit, 3}, is_distinct, doc) do
    quote do
      @doc unquote(doc)
      @spec unquote(name)(Column.t() | String.t(), term(), term(), term()) :: Column.t()
      def unquote(name)(col, arg1, arg2, arg3) do
        %Column{
          expr:
            {:fn, unquote(spark_name),
             [to_expr(col), lit_expr(arg1), lit_expr(arg2), lit_expr(arg3)], unquote(is_distinct)}
        }
      end
    end
  end

  defp generate_function(name, spark_name, {:lit_then_cols, 1}, is_distinct, doc) do
    quote do
      @doc unquote(doc)
      @spec unquote(name)(term(), [Column.t() | String.t()]) :: Column.t()
      def unquote(name)(lit_arg, cols) when is_list(cols) do
        args = [lit_expr(lit_arg) | Enum.map(cols, &to_expr/1)]
        %Column{expr: {:fn, unquote(spark_name), args, unquote(is_distinct)}}
      end
    end
  end

  defp generate_function(name, spark_name, {:lit_opt, defaults}, is_distinct, doc) do
    escaped_defaults = Macro.escape(defaults)

    quote do
      @doc unquote(doc)
      @spec unquote(name)(keyword()) :: Column.t()
      def unquote(name)(opts \\ []) do
        args =
          unquote(escaped_defaults)
          |> Enum.map(fn {key, default} -> Keyword.get(opts, key, default) end)
          |> Enum.reverse()
          |> Enum.drop_while(&is_nil/1)
          |> Enum.reverse()
          |> Enum.map(&lit_expr/1)

        %Column{expr: {:fn, unquote(spark_name), args, unquote(is_distinct)}}
      end
    end
  end

  defp generate_function(name, spark_name, {:col_opt, defaults}, is_distinct, doc) do
    escaped_defaults = Macro.escape(defaults)

    quote do
      @doc unquote(doc)
      @spec unquote(name)(Column.t() | String.t(), keyword()) :: Column.t()
      def unquote(name)(col, opts \\ []) do
        opt_args =
          unquote(escaped_defaults)
          |> Enum.map(fn {key, default} -> Keyword.get(opts, key, default) end)
          |> Enum.reverse()
          |> Enum.drop_while(&is_nil/1)
          |> Enum.reverse()
          |> Enum.map(&lit_expr/1)

        %Column{
          expr: {:fn, unquote(spark_name), [to_expr(col) | opt_args], unquote(is_distinct)}
        }
      end
    end
  end

  # --- Alias generation ---

  defp generate_alias(alias_name, primary_name, :zero) do
    quote do
      @doc "Alias for `#{unquote(primary_name)}/0`."
      @spec unquote(alias_name)() :: Column.t()
      def unquote(alias_name)(), do: unquote(primary_name)()
    end
  end

  defp generate_alias(alias_name, primary_name, :one_col) do
    quote do
      @doc "Alias for `#{unquote(primary_name)}/1`."
      @spec unquote(alias_name)(Column.t() | String.t()) :: Column.t()
      def unquote(alias_name)(col), do: unquote(primary_name)(col)
    end
  end

  defp generate_alias(alias_name, primary_name, :two_col) do
    quote do
      @doc "Alias for `#{unquote(primary_name)}/2`."
      @spec unquote(alias_name)(Column.t() | String.t(), Column.t() | String.t()) :: Column.t()
      def unquote(alias_name)(a, b), do: unquote(primary_name)(a, b)
    end
  end

  defp generate_alias(alias_name, primary_name, :three_col) do
    quote do
      @doc "Alias for `#{unquote(primary_name)}/3`."
      @spec unquote(alias_name)(
              Column.t() | String.t(),
              Column.t() | String.t(),
              Column.t() | String.t()
            ) :: Column.t()
      def unquote(alias_name)(a, b, c), do: unquote(primary_name)(a, b, c)
    end
  end

  defp generate_alias(alias_name, primary_name, :n_col) do
    quote do
      @doc "Alias for `#{unquote(primary_name)}/1`."
      @spec unquote(alias_name)([Column.t() | String.t()]) :: Column.t()
      def unquote(alias_name)(cols), do: unquote(primary_name)(cols)
    end
  end

  defp generate_alias(alias_name, primary_name, {:lit_opt, _defaults}) do
    quote do
      @doc "Alias for `#{unquote(primary_name)}/1`."
      @spec unquote(alias_name)(keyword()) :: Column.t()
      def unquote(alias_name)(opts \\ []), do: unquote(primary_name)(opts)
    end
  end

  defp generate_alias(alias_name, primary_name, {:col_opt, _defaults}) do
    quote do
      @doc "Alias for `#{unquote(primary_name)}/2`."
      @spec unquote(alias_name)(Column.t() | String.t(), keyword()) :: Column.t()
      def unquote(alias_name)(col, opts \\ []), do: unquote(primary_name)(col, opts)
    end
  end

  defp generate_alias(alias_name, primary_name, {:col_lit, 1}) do
    quote do
      @doc "Alias for `#{unquote(primary_name)}/2`."
      @spec unquote(alias_name)(Column.t() | String.t(), term()) :: Column.t()
      def unquote(alias_name)(col, a), do: unquote(primary_name)(col, a)
    end
  end

  defp generate_alias(alias_name, primary_name, {:col_lit, 2}) do
    quote do
      @doc "Alias for `#{unquote(primary_name)}/3`."
      @spec unquote(alias_name)(Column.t() | String.t(), term(), term()) :: Column.t()
      def unquote(alias_name)(col, a, b), do: unquote(primary_name)(col, a, b)
    end
  end

  defp generate_alias(alias_name, primary_name, {:col_lit, 3}) do
    quote do
      @doc "Alias for `#{unquote(primary_name)}/4`."
      @spec unquote(alias_name)(Column.t() | String.t(), term(), term(), term()) :: Column.t()
      def unquote(alias_name)(col, a, b, c), do: unquote(primary_name)(col, a, b, c)
    end
  end

  defp generate_alias(alias_name, primary_name, {:lit, 1}) do
    quote do
      @doc "Alias for `#{unquote(primary_name)}/1`."
      @spec unquote(alias_name)(term()) :: Column.t()
      def unquote(alias_name)(a), do: unquote(primary_name)(a)
    end
  end

  defp generate_alias(alias_name, primary_name, {:lit_then_cols, 1}) do
    quote do
      @doc "Alias for `#{unquote(primary_name)}/2`."
      @spec unquote(alias_name)(term(), [Column.t() | String.t()]) :: Column.t()
      def unquote(alias_name)(a, cols), do: unquote(primary_name)(a, cols)
    end
  end
end
