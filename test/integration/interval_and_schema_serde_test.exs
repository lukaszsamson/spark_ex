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
    case kind do
      :boolean ->
        :boolean

      :byte ->
        :byte

      :short ->
        :short

      :integer ->
        :integer

      :long ->
        :long

      :float ->
        :float

      :double ->
        :double

      :string ->
        :string

      :binary ->
        :binary

      :date ->
        :date

      :timestamp ->
        :timestamp

      :timestamp_ntz ->
        :timestamp_ntz

      :decimal ->
        {:decimal, value.precision, value.scale}

      :array ->
        {:array, spark_type_to_types(value.element_type)}

      :map ->
        {:map, spark_type_to_types(value.key_type), spark_type_to_types(value.value_type)}

      :struct ->
        fields =
          Enum.map(value.fields, fn field ->
            %{
              name: field.name,
              type: spark_type_to_types(field.data_type),
              nullable: field.nullable
            }
          end)

        {:struct, fields}

      _ ->
        :string
    end
  end
end
