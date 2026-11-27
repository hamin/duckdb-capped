# AGENTS.md

Guidance for coding agents working in this repository. `README.md` is the
user-facing description; this file is how to change the code safely.

## What this is

`capped` is a DuckDB storage extension: `ATTACH 'dir' AS s (TYPE capped)`
gives tables that forget. Each table is a set of immutable Parquet segments,
and retention (`ttl`, `max_bytes`, `max_rows`) drops whole segments. There is
no UPDATE or DELETE by design.

Two halves, joined by a C ABI:

| Path | Role |
|---|---|
| `crates/capped-core` | Rust "brain": policy parsing, manifest, seal and eviction planning, search planning. No DuckDB dependency. Built as a `staticlib` the CMake build links. |
| `src/`, `src/include/` | C++ "shell": catalog, schema, tables, transactions, write path, index sidecars. Owns files and everything DuckDB. |
| `src/include/capped.h` ↔ `crates/capped-core/src/capi.rs` | The ABI. Rust is authoritative; change both in the same commit. |
| `test/sql/*.test` | sqllogictests for the extension. |
| `examples/` | Runnable demos (`telemetry/`, `event_log/`). |
| `docs/` | `design.md` (why and how), `open-segments.md` (the write path), `updating-duckdb.md` (bumping the pin). Keep them true to the code; plans and work logs don't belong here. |

## Commands

```sh
cargo test                                   # Rust core (fast; no submodules needed)
cargo clippy --all-targets -- -D warnings    # CI gate
GEN=ninja make release                       # DuckDB + extension (slow the first time)
make test                                    # every sqllogictest (stages fts/vss on first run; needs network)
make test_search                             # just the search suite
make asan                                    # whole suite under AddressSanitizer
scripts/smoke.sh                             # end-to-end against the local duckdb CLI (same version)
```

Build outputs: `build/release/duckdb`, `build/release/test/unittest`,
`build/release/extension/capped/capped.duckdb_extension` (load it with
`duckdb -unsigned`).

Before calling a change done: `cargo test` and clippy for Rust changes,
`make release && make test` for anything touching `src/` or the ABI.

## Rules

- **DuckDB is pinned to v2.0.1.** The `duckdb` submodule sits at the tag,
  `extension-ci-tools` at `v2.0-cyanoptera`, and
  `.github/workflows/MainDistributionPipeline.yml` passes the same pair. Move
  them together. The Makefile's `OVERRIDE_GIT_DESCRIBE` fallback (v2.0.1) is what makes
  a shallow submodule build behave like the release; don't remove it
  (`docs/updating-duckdb.md`).
- **Only JSON strings and primitives cross the FFI.** Strings returned by Rust
  are freed with `capped_free_string`; failures return `CAPPED_ERR` with a
  message in `*out_err`. Never pass C++ objects or Rust structs across.
- **Policy lives in Rust, I/O lives in C++.** New retention or sealing logic
  goes in `capped-core` with unit tests; the shell only executes plans.
- **Parquet is the only source of truth.** fts/vss index sidecars are
  derived and best-effort: a missing or stale index must never lose rows or
  fail a scan.
- **Manifests are on-disk format.** Keep old manifests readable
  (`crates/capped-core/testdata/legacy_manifest.json` is the regression
  fixture); add fields with serde defaults.
- **Tests:** a behaviour change gets a sqllogictest in `test/sql/`; a core
  change gets a Rust unit test. Don't weaken an existing assertion to make a
  change pass.
- **Platforms:** Linux and macOS only. The musl, Windows and wasm targets are
  excluded in both `description.yml` and the CI workflow; keep those lists in
  step.
- No secrets, personal paths or machine names in code, tests, fixtures or
  docs; examples use temp directories.
