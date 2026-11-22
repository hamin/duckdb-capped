# Updating to a new DuckDB release

capped builds against one pinned DuckDB release (currently **v2.0.0**). The
Rust core (`crates/capped-core`) has no DuckDB dependency, so a DuckDB bump
only touches the C++ code in `src/`, the submodules and CI.

## 1. Move the pins together

Four places name the DuckDB version, and they must agree:

| Where | What to set |
|---|---|
| `duckdb` submodule | the new release tag |
| `extension-ci-tools` submodule | the matching branch (for v2.0.0 it is `v2.0-cyanoptera`) |
| `.github/workflows/MainDistributionPipeline.yml` | `duckdb_version`, `ci_tools_version`, and the `@ref` of the reusable workflow |
| `Makefile` | `OVERRIDE_GIT_DESCRIBE ?= <tag>` |

The extension-ci-tools ref is a **branch**, not a tag, so a plain
`git checkout` of it fails in a shallow clone. Fetch it explicitly:

```sh
git -C duckdb fetch --depth 1 origin tag v2.1.0 && git -C duckdb checkout v2.1.0
git -C extension-ci-tools fetch --depth 1 origin v2.1-<name> \
  && git -C extension-ci-tools checkout -B v2.1-<name> FETCH_HEAD
```

(`v2.1.0` and `v2.1-<name>` are placeholders; use the real release names.)

Then delete `build/` before configuring. Old CMake caches can carry settings
the new DuckDB rejects (the 1.5 caches set `CMAKE_CXX_STANDARD=11`, which 2.0
refuses).

## 2. Why the Makefile pins `OVERRIDE_GIT_DESCRIBE`

The `duckdb` submodule is a shallow clone. Without the override, DuckDB's
CMake counts commits on that clone and stamps the build as a dev version
(`v2.0.0-dev1`, with the commit hash as the extension version). Two things
then break: the official `fts` and `vss` builds refuse to load ("built
specifically for DuckDB version 'v2.0.0'"), and `capped.duckdb_extension`
gets a hash stamp instead of the release version. DuckDB's own CI sets the
same override for shallow checkouts.

## 3. The search tests need the official fts and vss builds

`test/sql/capped_search.test` and `capped_fragments_e2e.test` `LOAD fts` and
`vss`. The DuckDB test runner replaces `HOME` with a fresh temp directory on
every run, so anything installed under `~/.duckdb` is invisible, and
`require fts` can't be used either: in an out-of-tree build the runner only
satisfies `require` for statically linked code, so the file is reported as
skipped while testing nothing.

The Makefile's `remote-extensions` target handles this. It downloads each
extension once from `extensions.duckdb.org/<tag>/<platform>/` into
`build/remote-extensions/`, writes a test config that points the
`extension_directory` setting there, and exports `DUCKDB_TEST_CONFIG`.
`make test`, `make test_search` and `make asan` depend on it. Delete
`build/remote-extensions/` to fetch again after a bump.

## 4. ASan on macOS

`make asan` runs with `detect_stack_use_after_return=1`. On DuckDB 2.0, the
join-order optimiser in DuckDB core recurses deeper than before, and with
plain ASan frames it overflows the 512 KB macOS worker-thread stack in
`capped_fragments_e2e.test`. That flag moves instrumented locals to the heap.
`detect_container_overflow=0` covers a false positive in DuckDB core (a
vector copy inside the parquet extension's settings). Revisit both after a
bump; they are workarounds for DuckDB, not for capped.

## 5. What a bump usually breaks

The 1.5 → 2.0 port was all C++ API churn in `src/`; the Rust core and the C
ABI were untouched. The kinds of change to expect:

- names moving from `string` to `Identifier`, with no implicit conversion
  (wrap inputs in `Identifier(x)`, read outputs with `GetIdentifierName()`);
- renamed or reshaped table-function APIs (2.0 removed `named_parameters`;
  optional parameters now need explicit defaults in the `FunctionSignature`);
- catalog lookups and `TableCatalogEntry` gaining or losing parameters and
  pure virtual methods;
- headers that are now only forward-declared and need an explicit include.

Build with `GEN=ninja make release`, work through the compile errors
file by file, then require all of these to pass before committing:

```sh
cargo test
make test
make asan
scripts/smoke.sh
```

A version bump should not need test changes. If a sqllogictest fails, find
out whether DuckDB's behaviour changed or capped broke before editing the
expected output.

The 2.0 build still has deprecation warnings for `DataChunk::SetValue`
(about 55 call sites). They need a per-site rewrite, not a mechanical rename.
