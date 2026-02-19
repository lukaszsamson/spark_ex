defmodule SparkEx.LivebookTest do
  use ExUnit.Case, async: true

  alias Spark.Connect.DataType
  alias SparkEx.DataFrame

  if Code.ensure_loaded?(Kino) and Code.ensure_loaded?(Kino.Render) and
       Code.ensure_loaded?(Explorer.DataFrame) do
    defmodule FakeSession do
      use GenServer

      def start_link(opts) do
        GenServer.start_link(__MODULE__, opts)
      end

      @impl true
      def init(opts) do
        parent = Keyword.fetch!(opts, :parent)
        schema = Keyword.fetch!(opts, :schema)
        explain = Keyword.get(opts, :explain, {:ok, "== Physical Plan ==\nLocalTableScan"})
        explorer = Keyword.fetch!(opts, :explorer)
        {:ok, %{parent: parent, schema: schema, explain: explain, explorer: explorer}}
      end

      @impl true
      def handle_call({:execute_explorer, _plan, opts}, _from, state) do
        send(state.parent, {:execute_explorer_opts, opts})
        {:reply, state.explorer, state}
      end

      def handle_call({:analyze_explain, _plan, mode}, _from, state) do
        send(state.parent, {:analyze_explain_mode, mode})
        {:reply, state.explain, state}
      end

      def handle_call({:analyze_schema, _plan}, _from, state) do
        {:reply, state.schema, state}
      end
    end

    describe "SparkEx.Livebook helpers" do
      setup do
        schema = {:ok, test_schema()}
        explorer = {:ok, Explorer.DataFrame.new(%{"id" => [1, 2], "name" => ["a", "b"]})}

        {:ok, session} =
          FakeSession.start_link(
            parent: self(),
            schema: schema,
            explorer: explorer
          )

        df = %DataFrame{session: session, plan: {:sql, "SELECT 1"}}
        %{df: df}
      end

      test "preview returns Kino term and forwards max_rows", %{df: df} do
        assert %Kino.JS.Live{} = SparkEx.Livebook.preview(df, num_rows: 7)
        assert_receive {:execute_explorer_opts, opts}
        assert Keyword.get(opts, :max_rows) == 7
      end

      test "sample delegates to preview", %{df: df} do
        assert %Kino.JS.Live{} = SparkEx.Livebook.sample(df, num_rows: 3)
        assert_receive {:execute_explorer_opts, opts}
        assert Keyword.get(opts, :max_rows) == 3
      end

      test "explain returns Kino.Text and forwards explain mode", %{df: df} do
        assert %Kino.Text{} = SparkEx.Livebook.explain(df, mode: :simple)
        assert_receive {:analyze_explain_mode, :simple}
      end

      test "schema returns Kino.Text", %{df: df} do
        assert %Kino.Text{} = SparkEx.Livebook.schema(df)
      end
    end

    describe "Kino.Render implementation for SparkEx.DataFrame" do
      setup do
        schema = {:ok, test_schema()}
        explorer = {:ok, Explorer.DataFrame.new(%{"id" => [1], "name" => ["x"]})}

        {:ok, session} =
          FakeSession.start_link(
            parent: self(),
            schema: schema,
            explorer: explorer
          )

        df = %DataFrame{session: session, plan: {:sql, "SELECT 1"}}
        %{df: df}
      end

      test "renders dataframe to a Livebook term", %{df: df} do
        output = Kino.Render.to_livebook(df)
        assert output.type == :tabs
        assert output.labels == ["Schema", "Preview", "Explain", "Raw"]
        assert length(output.outputs) == 4
      end
    end

    defp test_schema do
      %DataType{
        kind:
          {:struct,
           %DataType.Struct{
             fields: [
               %DataType.StructField{
                 name: "id",
                 data_type: %DataType{kind: {:long, %DataType.Long{}}},
                 nullable: false
               },
               %DataType.StructField{
                 name: "name",
                 data_type: %DataType{kind: {:string, %DataType.String{}}},
                 nullable: true
               }
             ]
           }}
      }
    end
  else
    test "kino/explorer optional deps are not loaded in this test environment" do
      refute Code.ensure_loaded?(Kino.Render)
      refute Code.ensure_loaded?(Explorer.DataFrame)
    end
  end
end
