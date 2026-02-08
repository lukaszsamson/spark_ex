defmodule SparkEx.MixProject do
  use Mix.Project

  def project do
    [
      app: :spark_ex,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      aliases: aliases()
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
      {:protobuf, "~> 0.13"},

      # Utilities
      {:nimble_options, "~> 1.1"},
      {:jason, "~> 1.4"},

      # Observability
      {:telemetry, "~> 1.3"},

      # Optional - feature gated
      {:explorer, "~> 0.10", optional: true},
      {:kino, "~> 0.14", optional: true},

      # Dev/test
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp aliases do
    [
      "spark_ex.gen_proto": ["cmd priv/scripts/gen_proto.sh"]
    ]
  end
end
