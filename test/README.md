# Tests

`sql/` holds the extension's [sqllogictests](https://duckdb.org/docs/stable/dev/sqllogictest/intro):
one file per area (core DDL/DML and retention, concurrent transactions,
open-segment fragments, maintenance, observability, search).

```sh
GEN=ninja make release
make test                                         # every file
build/release/test/unittest "test/sql/capped.test"  # one file
make test_search                                  # the search suite (fts/vss)
make asan                                         # everything under AddressSanitizer
```

The search tests `LOAD` the official `fts` and `vss` builds; the Makefile
downloads them into `build/remote-extensions/` on first use (see
[docs/updating-duckdb.md](../docs/updating-duckdb.md) §3).

The Rust core has its own unit tests: `cargo test` from the repo root.
