# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Stable plan_ids assigned at DataFrame construction.** Every `SparkEx.DataFrame`
  now allocates a stable `plan_id` (per-process `:counters` ref, 0-based,
  matching PySpark's `_fresh_plan_id`) when the plan tuple is wrapped via
  `SparkEx.Internal.PlanIds.wrap/1`. `DataFrame.col/2`, `col_regex/2`, and
  `metadata_column/2` capture the wrapped plan so column references carry
  their source DataFrame's id end-to-end. This resolves the BUGS_PLAN_5
  Stream A "known limitation" (same DataFrame as join child + subquery
  reference) — both sites resolve through the same id, no synthetic remap.
- **Self-join wire format.** `df.join(df, …)` now produces
  `join.left.common.plan_id == join.right.common.plan_id` (matches PySpark's
  per-LogicalPlan-instance plan_id). Previously each side received a
  distinct counter id from the encoder. Tests pinning distinct ids should
  match through `SparkEx.Test.PlanHelpers.unwrap_plan/1` or use a `_`
  wildcard.
- DataFrames must be constructed via `SparkEx.DataFrame.new/2` (or one of
  the public session APIs that wrap it) rather than `%DataFrame{plan: …}`
  struct literals — `col/2`, `col_regex/2`, `metadata_column/2` now raise
  `ArgumentError` on unstamped plans with a message pointing at `new/2`.

## [0.1.1] - 2026-03-06

### Changed

- Explorer is now a required dependency (needed for Arrow IPC result decoding)
- `decimal` is now an explicit required dependency (was only transitive)
- `Session.stop/1` is now idempotent — stopping an already-stopped session returns `:ok`
- `Channel.connect/2` accepts extra gRPC options for passthrough to the adapter

### Fixed

- Compilation failure when installing without optional Explorer dependency
- Integration test compatibility with Spark 3.5 (nil `error_class` in remote errors)
- Structured streaming result collection via proper gRPC streaming
- Flaky test cleanup race condition in `on_exit` callbacks
- Multiple reward-hacked tests with tautological assertions, swallowed errors, and contradictory conditions

### Removed

- `connect_timeout` option (was never wired into anything; use `grpc_opts` passthrough instead)

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

[0.1.1]: https://github.com/lukaszsamson/spark_ex/releases/tag/v0.1.1
[0.1.0]: https://github.com/lukaszsamson/spark_ex/releases/tag/v0.1.0
