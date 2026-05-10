defmodule SparkEx.Integration.IntervalAndSchemaSerdeTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.{DataFrame, Types}

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  test "interval types appear in schema", %{session: session} do
    df = SparkEx.sql(session, "SELECT INTERVAL 1 YEAR 2 MONTH AS ym")

    assert {:ok, %Spark.Connect.DataType{kind: {:struct, struct}}} = DataFrame.schema(df)

    field = Enum.find(struct.fields, &(&1.name == "ym"))
    assert match?(%Spark.Connect.DataType{kind: {:year_month_interval, _}}, field.data_type)
  end

  test "nested schema inference and JSON serde", %{session: session} do
    df =
      SparkEx.sql(
        session,
        """
        SELECT
          named_struct('id', 1, 'tags', array('a', 'b')) AS st,
          map('k', 2) AS mp
        """
      )

    assert {:ok, schema} = DataFrame.schema(df)
    %Spark.Connect.DataType{kind: {:struct, struct}} = schema

    fields =
      Enum.map(struct.fields, fn field ->
        %{
          name: field.name,
          type: spark_type_to_types(field.data_type),
          nullable: field.nullable
        }
      end)

    json = Types.to_json({:struct, fields})
    decoded = Jason.decode!(json)

    assert decoded["type"] == "struct"
    assert length(decoded["fields"]) == 2
  end

  defp spark_type_to_types(%Spark.Connect.DataType{kind: {kind, value}}) do
    spark_kind_to_types(kind, value)
  end

  defp spark_kind_to_types(kind, _value)
       when kind in [
              :boolean,
              :byte,
              :short,
              :integer,
              :long,
              :float,
              :double,
              :string,
              :binary,
              :date,
              :timestamp,
              :timestamp_ntz
            ] do
    spark_scalar_kind_to_types(kind)
  end

  defp spark_kind_to_types(:decimal, value), do: {:decimal, value.precision, value.scale}
  defp spark_kind_to_types(:array, value), do: {:array, spark_type_to_types(value.element_type)}

  defp spark_kind_to_types(:map, value),
    do: {:map, spark_type_to_types(value.key_type), spark_type_to_types(value.value_type)}

  defp spark_kind_to_types(:struct, value) do
    fields =
      Enum.map(value.fields, fn field ->
        %{
          name: field.name,
          type: spark_type_to_types(field.data_type),
          nullable: field.nullable
        }
      end)

    {:struct, fields}
  end

  defp spark_kind_to_types(_kind, _value), do: :string

  defp spark_scalar_kind_to_types(:boolean), do: :boolean
  defp spark_scalar_kind_to_types(:byte), do: :byte
  defp spark_scalar_kind_to_types(:short), do: :short
  defp spark_scalar_kind_to_types(:integer), do: :integer
  defp spark_scalar_kind_to_types(:long), do: :long
  defp spark_scalar_kind_to_types(:float), do: :float
  defp spark_scalar_kind_to_types(:double), do: :double
  defp spark_scalar_kind_to_types(:string), do: :string
  defp spark_scalar_kind_to_types(:binary), do: :binary
  defp spark_scalar_kind_to_types(:date), do: :date
  defp spark_scalar_kind_to_types(:timestamp), do: :timestamp
  defp spark_scalar_kind_to_types(:timestamp_ntz), do: :timestamp_ntz
end
