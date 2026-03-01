# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-03-02

### Added

- Native Elixir client for Apache Spark via the Spark Connect protocol
- gRPC-based communication with Spark Connect server
- DataFrame API with support for selects, filters, joins, aggregations, and sorting
- Column expression DSL for building Spark expressions
- Streaming query support with retry policies
- Session management (create, attach, reset, release)
- Telemetry integration for observability
- Optional Explorer integration for converting Spark results to Explorer DataFrames
- Optional Kino integration for Livebook rendering
- Function registry for built-in Spark functions
- Livebook demo notebook

[0.1.0]: https://github.com/lukaszsamson/spark_ex/releases/tag/v0.1.0
