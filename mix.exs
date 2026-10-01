defmodule SparkEx.MixProject do
  use Mix.Project

  @version "0.2.0"
  @source_url "https://github.com/lukaszsamson/spark_ex"

  def project do
    [
      app: :spark_ex,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      aliases: aliases(),
      name: "SparkEx",
      description: "Native Elixir client for Apache Spark via the Spark Connect protocol",
      package: package(),
      docs: docs(),
      dialyzer: [plt_local_path: "priv/plts"],
      source_url: @source_url,
      homepage_url: @source_url
    ]
  end

  def application do
    [
      mod: {SparkEx.Application, []},
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      # gRPC client + protobuf
      {:grpc, "~> 0.9"},
      {:protobuf, "~> 0.16.1 or ~> 0.17"},

      # Utilities
      {:decimal, "~> 1.0 or ~> 2.0"},
      {:nimble_options, "~> 1.1"},
      {:jason, "~> 1.4"},

      # Observability
      {:telemetry, "~> 1.3"},

      # Arrow IPC decoding for result materialization
      {:explorer, "~> 0.10"},

      # Optional - Livebook rendering
      {:kino, "~> 0.14", optional: true},

      # Dev/test
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.35", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url},
      files:
        ~w(lib priv/proto priv/scripts docs notebooks .formatter.exs mix.exs README.md CHANGELOG.md LICENSE)
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      extras: [
        "README.md",
        "docs/spark_42.md",
        "CHANGELOG.md",
        "notebooks/spark_ex_demo.livemd",
        "LICENSE"
      ],
      skip_code_autolink_to: ["Spark.Connect.GetStatusResponse"],
      # The experimental API intentionally returns a generated protobuf type.
      # Generated modules are hidden from ExDoc; retain the precise public specs.
      skip_undefined_reference_warnings_on: [
        "SparkEx.get_operation_statuses/3",
        "SparkEx.Session.get_operation_statuses/3"
      ]
    ]
  end

  defp aliases do
    [
      "spark_ex.gen_proto": ["cmd priv/scripts/gen_proto.sh"]
    ]
  end
end
