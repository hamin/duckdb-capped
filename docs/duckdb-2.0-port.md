# DuckDB 2.0 port survey (Job C, task C1)

Path note (E1b, 2026-11-16): the extension template now lives at the repository
root; `shell/` was removed. Operational path references below have been updated
to the new layout; remaining `shell/...` paths are historical records of the
pre-move layout (the survey and the port itself were done while it existed).

Survey date: 2026-11-12. Read-only survey: nothing under `capped/` or `lotel/` was changed
except this file. The trial build ran from a scratch copy (`$TMP/opencode/c1-survey/capped-2.0`).

## Recommendation (top line)

- **Target DuckDB tag: `v2.0.0`** (commit `d54cbbe3df17e8e5ce68312f483b4155836112aa`,
  released 2026-10-21). It is the **only** v2.0.x tag that exists — no patch release yet, so
  there is nothing newer to pick:
  `git ls-remote --tags https://github.com/duckdb/duckdb 'refs/tags/v2.0*'` →
  `d54cbbe3df...  refs/tags/v2.0.0` (single line). Re-check for `v2.0.1` before C2 starts; if
  it lands, take it plus the matching crate patch.
- **Rust `duckdb` crate for 2.0.0 exists: `1.20000.1`** — **C3 is not blocked.**
- **`otlp` (community) and `fts` (core) both install and load for v2.0.0** on
  osx-arm64 (verified below).

## 1. Versions (and where each was found)

### 1a. What duckdb-otlp targets

- `duckdb-otlp/.github/workflows/pages.yml:39` — `DUCKDB_VERSION: v2.0.0`;
  `:45` — `_extension_distribution.yml@v2.0-cyanoptera`; `:48-49` —
  `duckdb_version: v2.0.0`, `ci_tools_version: v2.0-cyanoptera`.
- `duckdb-otlp/.github/workflows/MainDistributionPipeline.yml:36,41,43,44` — same pair
  (`v2.0.0` + `v2.0-cyanoptera`), plus the code-quality workflow at `:361-363`.

Upstream tag check (this session):

```
$ git ls-remote --tags https://github.com/duckdb/duckdb 'refs/tags/v2.0*'
d54cbbe3df17e8e5ce68312f483b4155836112aa	refs/tags/v2.0.0
```

**Recommend `v2.0.0`.** Pair it with extension-ci-tools tag `v2.0-cyanoptera`
(`c286b12cee2aa16b714e76d0b46e530fcdb6dc05`, from
`git ls-remote https://github.com/duckdb/extension-ci-tools`).

### 1b. Rust `duckdb` crate

- `cargo search duckdb` / `cargo info duckdb` → latest `duckdb = "1.20000.1"`
  (`libduckdb-sys` and `duckdb-loadable-macros` at the same number).
- duckdb-rs GitHub release `v1.20000.1` (published 2026-10-28, branch `v2.0-cyanoptera`)
  states: *"DuckDB is unchanged at **v2.0.0** (`d54cbbe3df`)"* — the same commit as the
  upstream v2.0.0 tag.
- The version encoding is confirmed by the crate's own unit test:
  `libduckdb-sys-1.20000.1/src/build_version.rs:30` —
  `assert_eq!(duckdb_version_from_pkg_version("1.20000.0"), "2.0.0");`
  (and `:29` confirms today's lotel pin: `"1.10506.0" == "1.5.6"`). So the 2.0 version
  string is **`1.20000.x`**, i.e. lotel's `=1.10506.1` becomes `=1.20000.1`.
- `libduckdb-sys-1.20000.1.crate` ships `duckdb.tar.gz` (the bundled v2.0.0 sources), so
  the `bundled` feature needs no network at build time.
- lotel's feature list survives: `cargo add --dry-run duckdb@1.20000.1
  --features bundled,parquet,json,r2d2,chrono` resolves all five (`bundled`, `chrono`,
  `json`, `parquet`, `r2d2` all activate).

### 1c. otlp / fts availability for v2.0.0

Downloaded the official CLI `duckdb-cli-osx-arm64.tar.gz` from the v2.0.0 GitHub release
into the scratch dir (`./duckdb --version` → `v2.0.0 (Cyanoptera) d54cbbe3df`):

```
$ ./duckdb :memory: -unsigned -c "SET autoinstall_known_extensions=false;" \
    -c "INSTALL otlp FROM community;" -c "INSTALL fts;" \
    -c "SELECT extension_name, installed, install_mode FROM duckdb_extensions()
        WHERE extension_name IN ('otlp','fts');"
| extension_name:VARCHAR | installed:BOOLEAN | install_mode:VARCHAR |
|---|---|---|
| fts | true | REPOSITORY |
| otlp | true | REPOSITORY |
```

Both were fetched to `~/.duckdb/extensions/v2.0.0/osx_arm64/`
(`otlp.duckdb_extension` 46,986,942 B; `fts.duckdb_extension` 11,804,542 B) and
`LOAD otlp; LOAD fts;` then succeeded. C3 can fetch both for this version.

## 2. Trial build (scratch copy, real repo untouched)

Setup: copied `capped/shell/` (minus `build/`, submodules, caches) plus the Rust workspace
(`Cargo.toml`, `Cargo.lock`, `crates/`) into
`$TMP/opencode/c1-survey/capped-2.0/`; in the copy, `shell/duckdb` is a fresh shallow clone
at tag `v2.0.0` and `shell/extension-ci-tools` a fresh shallow clone at `v2.0-cyanoptera`.
`git -C capped/shell/duckdb describe --tags` still prints `v1.5.6` in the real repo.

Command: `GEN=ninja make release` (stage 1) then `ninja -k 0` in `build/release` (stage 2,
to collect every error).

- DuckDB 2.0.0 core, parquet, json, icu, the loadable test extensions, `unittest`, the
  shell, and **the Rust core (`libcapped_core.a`, target/capi)** all built clean. The Rust
  C ABI is untouched by the port, as the plan expects.
- **Total wall time: 345 s** for configure + both build stages (Apple M-series, ninja,
  ccache-warm from the 1.5.6 builds; a cold build will take longer).
- **All failures are in capped's own 8 C++ files.** 55 compile errors total
  (= per TU; each file compiled twice, static + loadable, and the header error repeats
  per TU). Per file: capped_catalog.cpp 4, capped_extension.cpp 19, capped_schema.cpp 15,
  capped_table.cpp 8, capped_write.cpp 3, capped_transaction.cpp 5, capped_index.cpp 1
  (header error only), capped_storage_info.cpp 0.
- `make test` was **not run** — the extension .o files never compiled. See §4.

Every fix below was read against the v2.0.0 source in the scratch tree (file:line given)
and, where relevant, against the 1.5.6 tree to classify the change.

**Context that shapes the whole port:** DuckDB 1.5.6 already carries the `Identifier`
class with *deliberately implicit* string conversions (`1.5.6/src/include/duckdb/common/
identifier.hpp:32-41` carries upstream's own `NOTE(backport)` comment: 2.0 makes these
explicit and every 2.0 API takes/returns `Identifier`; write exactly the casts 2.0 needs and
they compile on both). So most of §3.1 is mechanical: `Identifier(x)` at inputs,
`x.GetIdentifierName()` at outputs.
`2.0/src/include/duckdb/common/identifier.hpp:29-69` — explicit ctors from
`string`/`string_view`, explicit `operator const string &()` (`:61`), `GetIdentifierName()`
(`:68`); implicit only from `const char*` literals (`:30`).

### 3.1 Group 1 — `string` ↔ `Identifier` no longer implicit (changed signatures)

2.0 evidence: `catalog/catalog.hpp:118` (`GetCatalog(ClientContext&, const Identifier&)`),
`catalog.hpp:155` (`GetName()` → `const Identifier&`), `catalog/catalog_entry.hpp:49`
(`Identifier name;`), `parser/column_definition.hpp:27,44`
(`ColumnDefinition(Identifier, LogicalType)`, `Name()` → `const Identifier&`),
`main/appender.hpp:147-148` (all name params `Identifier`),
`function/table_function.hpp:345-346` (`table_function_bind_t` ...
`vector<Identifier> &names`).

| Site | Fix (verified against 2.0) |
|---|---|
| `capped_extension.cpp:156` `Catalog::GetCatalog(context, catalog_name)` | `Catalog::GetCatalog(context, Identifier(catalog_name))` |
| `capped_extension.cpp` — every `*Bind(...)` used at 269, 396, 451, 527/529, 714, 825, 991, 994, 1005, 1010: last param `vector<string> &names` | change those bind callbacks' signature to `vector<Identifier> &names`; `names.emplace_back("literal")` etc. keep working (implicit from literals) |
| `capped_table.cpp:120` `ColumnDefinition(name.Str(), ...)` | `ColumnDefinition(Identifier(name.Str()), ...)` |
| `capped_table.cpp:187` `sys.GetEntry<TableFunctionCatalogEntry>(context, DEFAULT_SCHEMA, fname, RETURN_NULL)` | modern overload: `sys.GetEntry<TableFunctionCatalogEntry>(context, QualifiedName(sys.GetName(), Identifier::DefaultSchema(), Identifier(fname)), OnEntryNotFound::RETURN_NULL)` — `catalog.hpp:455` + `qualified_name.hpp:37` |
| `capped_table.cpp:217,226` (+ the `%s` uses of `name` at 218-221, 232): `name` is now `CatalogEntry::name`, an `Identifier` | pass `name.GetIdentifierName()` to `HasBufferFor` / `ReadManifest` / the `NotImplementedException` |
| `capped_schema.cpp:131,148,150,153,171,173,174,175,176` `base.table` | `base.GetTableName().GetIdentifierName()` — `create_table_info.hpp:26-28` |
| `capped_schema.cpp:163` `CappedEscapeJson(col.Name())` | `CappedEscapeJson(col.Name().GetIdentifierName())` |
| `capped_schema.cpp:192,201` `info.name` (DropInfo) | `info.GetQualifiedName().Name().GetIdentifierName()` — `drop_info.hpp:44-46` |
| `capped_write.cpp:1331` `result["Table"] = entry.name;` | `entry.name.GetIdentifierName()` |
| `capped_transaction.cpp:48,54,56` `StringUtil::Lower(entry.name)`, `buffer->table = entry.name`, `column_names.push_back(col.Name())` | append `.GetIdentifierName()` at each |
| `capped_transaction.cpp:76` `make_uniq<Appender>(*work_con, "temp", "main", buffer->BufferTableName())` | wrap the 4th arg: `Identifier(buffer->BufferTableName())` (literals `"temp"`/`"main"` are fine as-is) |

### 3.2 Group 2 — `TableFunction::named_parameters` removed (renamed API)

1.5.6: `function/function.hpp:210` — `named_parameter_type_map_t named_parameters;` on the
`Function` base. 2.0 removed it; named parameters are now declared in the function's
`FunctionSignature`: `2.0/src/include/duckdb/function/function.hpp:134-139`
(`FunctionParameterKind`), `:142-149` (`FunctionParameter(Identifier, LogicalType,
optional<Value>, kind)`), `:349-351` —
`FunctionSignature::AddKeywordOnly(Identifier, LogicalType, optional<Value>)`, plus the
signature-taking ctors at `function/table_function.hpp:633-640`. Canonical core usage:
`src/function/table/glob.cpp:86`
(`TableFunction("glob", FunctionSignature().AddPositionalOnly("pattern",
LogicalType::VARCHAR), ...)`). The **bind-side read is unchanged** —
`TableFunctionBindInput::named_parameters` still exists
(`table_function.hpp:114,122`; duckdb-otlp reads it the same way,
`duckdb-otlp/src/otlp_start_stop.cpp:87-88`).

Sites: `capped_extension.cpp:827` (`rebuild`), `:989` (`k` via `add_named` lambda),
`:1008-1009` (`k`, `vector_column`), `:1013-1014`.

Fix pattern (example for `capped_index`):
`TableFunction index_fn("capped_index", FunctionSignature()
.AddPositionalOnly("db", LogicalType::VARCHAR).AddPositionalOnly("table",
LogicalType::VARCHAR).AddKeywordOnly("rebuild", LogicalType::BOOLEAN,
Value::BOOLEAN(false)), IndexBuildFunc, IndexBuildBind, IndexBuildInitGlobal);`
— delete the `named_parameters[...]` lines.

**Corrected in C2** (the original pattern above, omitting the default value, does not
work): in 2.0 a parameter is *required* unless it is variadic or carries a default —
`FunctionSignature::GetRequiredParameterCount` (`function.hpp`, "if (!param.IsVariadic()
&& !param.HasDefaultValue()) result++") counts keyword-only parameters too. So every
optional parameter must be registered **with the default the bind code used for an
absent parameter**, or calls that omit it fail to bind. Also pick the right kind:
1.x `named_parameters` entries were name-only (`KEYWORD_ONLY`), but `k` additionally had
a 4-positional-arg overload, so `k` must be a **STANDARD** parameter
(`AddParameter("k", BIGINT, Value::BIGINT(10))`) — passable positionally *and* by name.
`rebuild` → `AddKeywordOnly(..., Value::BOOLEAN(false))`,
`vector_column` → `AddKeywordOnly(..., Value(""))`,
`k` → `AddParameter("k", LogicalType::BIGINT, Value::BIGINT(10))` (bind code's default,
`ReadSearchK`). With defaults, the 3-arg and 4-arg positional overloads collapse into
one signature each (no more `fts_k`/`vec_k`). The bind-side `named_parameters.find(...)`
reads keep working: the binder fills defaults for omitted parameters.

### 3.3 Group 3 — `TableFunctionBindInput` constructor changed (changed signature)

`capped_table.cpp:253-262` builds one manually to drive `parquet_scan`'s bind. 2.0 ctor
(`function/table_function.hpp:113-127`): param 2 is `named_argument_map_t &` (was
`named_parameter_map_t &`; the new alias is `common/named_parameter_map.hpp:20` —
`InsertionOrderPreservingMap<Value, Identifier, identifier_map_t<idx_t>>`), param 4 is
`vector<Identifier> &`, and param 7 is now **`BoundTableFunction &`** (was `TableFunction
&`). Fix: `named_argument_map_t named_parameters;`, `vector<Identifier> input_table_names;`
and `scratch_names`, and pass a wrapped function — `BoundTableFunction` has the explicit
copy ctor `BoundTableFunction(const TableFunction&)` (`table_function.hpp:651`):
`BoundTableFunction bound_scan(scan); ... scan.bind(context, bind_input, ...)`.

**Found in C2 (not in C1's survey):** `LookupParquetScan` copies the function out of the
catalog, but in 2.0 `FunctionSet<T>::functions` is `vector<shared_ptr<const T>>`
(`function/function_set_base.hpp:28-30` — overloads are immutable and shared). `return
overloads[0];` no longer compiles; use `return *overloads[0];` (copy so the caller can
own a mutable function).

### 3.4 Group 4 — `Catalog::TryLookupEntryInternal` lost its `schema` parameter (changed signature)

1.5.6 `catalog/catalog.hpp:439-440`: `virtual CatalogEntryLookup TryLookupEntryInternal
(CatalogTransaction, const string &schema, const EntryLookupInfo &)`.
2.0 `catalog/catalog.hpp:537-539`: `(CatalogTransaction transaction, const EntryLookupInfo
&lookup_info)` — "using the schema carried in the lookup_info".
Affected: `src/include/capped_catalog.hpp:100-101` (the `override` that triggers
"non-virtual member function marked 'override' hides virtual member function" in **all 7
files** that include the header) and its definition at `src/capped_catalog.cpp:176-184`.
Fix: drop the `const string &schema` param from both; take the schema from
`lookup_info.GetSchema()` (`catalog/entry_lookup_info.hpp:48`, returns
`const Identifier&`, empty when unqualified) and keep the CI check against `"main"` via
`StringUtil::CIEquals(lookup_info.GetSchema().GetIdentifierName(), "main")` (treat empty
like "main" or per current semantics). `CatalogEntryLookup` itself is unchanged
(`entry_lookup_info.hpp:63-71`).

### 3.5 Group 5 — `TableCatalogEntry::GetColumns()` became pure virtual (new pure virtual)

1.5.6 `catalog_entry/table_catalog_entry.hpp:80`: plain virtual.
2.0 `table_catalog_entry.hpp:88`: `virtual const ColumnList &GetColumns() const = 0;` →
`error: allocating an object of abstract class type 'duckdb::CappedTableEntry'`
(helper.hpp:77, instantiated at `capped_schema.cpp:83`).
Fix: in `CappedTableEntry` add
`const ColumnList &GetColumns() const override { return create_info->columns; }` —
`create_info` is already stored (`src/include/capped_table.hpp:41`),
`CreateTableInfo::columns` is a `ColumnList` (`create_table_info.hpp:32`), and the base ctor
`TableCatalogEntry(Catalog&, SchemaCatalogEntry&, CreateTableInfo&)` is unchanged
(`table_catalog_entry.hpp:69`; capped's call compiled fine).

### 3.6 Group 6 — `ParsedData` name fields folded into `QualifiedName` (removed fields)

- `CreateSchemaInfo::schema` — 1.5.6 had a settable field; 2.0
  `parser/parsed_data/create_schema_info.hpp` has none: the new schema lives in the
  qualified-name `Schema()` slot (`src/parser/parsed_data/create_schema_info.cpp:9-12`)
  and `CreateInfo`'s ctor already defaults it to `Identifier::DefaultSchema()`
  (`create_info.hpp:33-37`). Fix at `capped_catalog.cpp:18`: **delete the line** — the
  default is already `main`.
- `CreateTableInfo::table` — 1.5.6 `create_table_info.hpp:25` `string table;`; 2.0 has
  `GetTableName()`/`SetTableName()` (`create_table_info.hpp:26-31`).
  `capped_schema.cpp` sites covered in §3.1.
- `DropInfo::name` — 1.5.6 `drop_info.hpp:35` `string name;`; 2.0 keeps it only inside a
  private `QualifiedName` with `GetQualifiedName()` (`drop_info.hpp:44-46, 71-72`).
  `capped_schema.cpp:192,201` covered in §3.1.
- `CreateTableInfo` 3-arg ctor removed — 1.5.6 had
  `CreateTableInfo(catalog, schema, table)`; 2.0 offers
  `CreateTableInfo(QualifiedName)` (`create_table_info.hpp:23`) or
  `CreateTableInfo(SchemaCatalogEntry&, const Identifier&)` (`:24`).
  Fix at `capped_table.cpp:101`
  (`make_uniq<CreateTableInfo>(catalog.GetName(), "main", table)`):
  `make_uniq<CreateTableInfo>(QualifiedName(catalog.GetName(),
  Identifier::DefaultSchema(), Identifier(table)))` — `QualifiedName(catalog, schema,
  name)` ctor at `parser/qualified_name.hpp:37`; `Catalog::GetName()` already returns
  `const Identifier&` (`catalog.hpp:155`).

### 3.7 Group 7 — `BaseQueryResult::types` became private (changed access)

1.5.6 `main/query_result.hpp:35-38`: public field. 2.0 `query_result.hpp:79`: private,
with `const vector<LogicalType> &GetTypes() const` (`:52`).
Fix at `capped_write.cpp:204`: `result.GetTypes()`.

### 3.8 Group 8 — `PhysicalPlanGenerator` only forward-declared (header moved)

`capped_catalog.cpp:151,153` (`planner.ResolveDefaultsProjection(...)`,
`planner.Make<CappedInsert>(...)`) — in 2.0 `catalog.hpp` only forward-declares the class
(`catalog.hpp:77`), so the full header must be included. The class and both methods live in
`2.0/src/include/duckdb/execution/physical_plan_generator.hpp`
(`Make<T>` at `:52`, `ResolveDefaultsProjection(LogicalInsert&, PhysicalOperator&)` at
`:130` — signature unchanged).
Fix: add `#include "duckdb/execution/physical_plan_generator.hpp"` to
`src/capped_catalog.cpp`.

### 3.9 Group 9 — `ConstantExpression::value` replaced by `Literal` (renamed API)

1.5.6 `parser/expression/constant_expression.hpp:25`: `Value value;` (public).
2.0 `constant_expression.hpp:24-29`: ctor takes `Literal`, member is private, accessor is
`const Literal &GetLiteral() const`; `Value`-based ctor explicitly deleted.
`Literal` (`parser/literal.hpp:43-88`) carries a `LiteralKind` + raw `text` and offers
`Value ToValue() const` (`:74`) and `string ToString() const` (`:76`).
Fix at `capped_schema.cpp:114` (`auto value = constant.value.ToString();`):
`auto value = constant.GetLiteral().ToValue().ToString();` — preserves the old
`Value::ToString()` semantics that capped-core's policy parser expects
(`expr.GetExpressionType() != ExpressionType::VALUE_CONSTANT` at `:110` still compiles).

### 3.10 Deprecation warnings (not errors, 63 of them — worth clearing in C2)

- `DataChunk::SetCardinality` — deprecated in favour of `CheckCardinality` /
  `SetChildCardinality` (`2.0 common/types/data_chunk.hpp:77`). Used in
  capped_extension.cpp (e.g. `:175`, `:883`) and capped_write.cpp (`:1321`, `:1336`).
- `DataChunk::SetValue` — prefer `Vector::Append`/`Vector::SetValue`
  (`data_chunk.hpp:88`). Used in capped_extension.cpp (`:886-888`) and capped_write.cpp.
- `EntryLookupInfo(CatalogType, Identifier)` — deprecated; pass a `QualifiedName`
  (`catalog/entry_lookup_info.hpp:27-30`). Site: `capped_table.cpp:273` →
  `EntryLookupInfo lookup_info(CatalogType::TABLE_ENTRY, QualifiedName(name));`
  (`QualifiedName(Identifier)` is implicit, `qualified_name.hpp:34`).

Warnings are not errors in this build, but keeping the count near zero makes the asan/CI
runs readable.

**Done in C2 (mechanical only, per plan):**
- `SetCardinality(n)` → `SetCardinalityUnsafe(n)`, 11 sites (capped_extension.cpp ×9,
  capped_table.cpp ×1, capped_write.cpp ×1). Verified identical: the deprecated wrapper
  body is literally `SetCardinalityUnsafe(count_p)` (`data_chunk.hpp:68-77`) — the
  non-Unsafe variants (`SetChildCardinality`/`CheckCardinality`) resize child vectors
  and would change behaviour for these fill-then-set-emitted chunks.
- `EntryLookupInfo(CatalogType::TABLE_ENTRY, QualifiedName(name))`, 1 site
  (capped_table.cpp).

**Left for the owner (not one-line mechanical):**
- `DataChunk::SetValue(col, row, Value)` — 55 sites (capped_extension.cpp 54,
  capped_write.cpp 1). Each result-emitting function must be restructured to
  `output.data[col].Append/SetValue` on the underlying `Vector`s, which changes how
  cardinality is established; a blind sed would alter behaviour. Warnings remain
  visible in builds.

## 4. Test failures

**Not reached** — the build failed before producing the extension objects, so
`make test` was not run (per task instructions). What *is* known: the DuckDB-side test
binary itself built (`build/release/test/unittest`, `test/*` infra fine), and running
capped's own suite against that capped-free binary reports

```
$ ./build/release/test/unittest "test/sql/capped.test"
All tests were skipped (total skipped 1)
Skipped tests for the following reasons:
require capped: 1
```

i.e. the harness works; only the extension is missing. **Risk to watch in C2:** the
7 sqllogictests in `test/sql/` assert on exact error strings
(`CatalogException`/`IOException` texts). 2.0 touched catalog error formatting in places
(the `CatalogException::EntryAlreadyExists` helper, `create_schema_info.cpp:37`, is new),
so expect a few message diffs; per the plan, **stop and report** rather than editing tests.

## 5. Extension template & CI changes

Compared `capped/shell/` against a fresh clone of `duckdb/extension-template` @ `main`
(commit `fd1d3a78`, whose duckdb submodule pins `d54cbbe3df` = **v2.0.0** exactly — there
is no separate v2.0 branch/tag of the template; extension-template now tracks everything
through main + the ci-tools tag):

- `.github/workflows/ExtensionTemplate.yml` and `MainDistributionPipeline.yml`:
  **byte-identical to capped's copies** (`diff` → no output). Nothing to change; the DuckDB
  version they build comes from the submodules (`<submodule_version>`). No workflow file
  contains a hard-coded 1.5 version (grep for `1.5`/`variegata` → no hits).
- `Makefile`: only capped's own additions differ (`EXT_NAME=capped`, `test_search`,
  `asan`). The 2.0 template Makefile is the same 8-line file. Both custom targets stay
  valid: `RELEASE_SANITIZER` is still a 2.0 option (`2.0/CMakeLists.txt:342`), and
  `BUILD_FLAGS`/`GENERATOR`/`VCPKG_MANIFEST_FLAGS` still exist in the 2.0 ci-tools
  makefile.
- `CMakeLists.txt`: the 2.0 template adds `find_package(OpenSSL)` + vcpkg links around an
  otherwise-identical file. capped intentionally uses neither (`vcpkg.json` present but no
  toolchain set, build never touched vcpkg). Keep capped's Rust-staticlib customisations
  verbatim; `build_static_extension`/`build_loadable_extension` still work (the link steps
  ran in the trial build).
- `extension_config.cmake`: unchanged pattern; capped's extra
  `duckdb_extension_load(parquet)` built fine against 2.0.
- Submodules are the only real CI/version change:
  - `duckdb`: `v1.5.6` (`069cc9f9`) → **tag `v2.0.0`** (`d54cbbe3df`);
  - `extension-ci-tools`: `5dd9e7c` (1.5-era grafted commit) → **branch
    `v2.0-cyanoptera`** (`c286b12`) — **corrected in C2: `v2.0-cyanoptera` is a branch,
    not a tag** (`git ls-remote` shows no such tag; `git fetch --tags` doesn't see it).
    Reproduce with `git fetch --depth 1 origin v2.0-cyanoptera && git checkout -B
    v2.0-cyanoptera FETCH_HEAD`.
  No `.gitmodules` change is required (template also tracks `main`/upstream URLs).
- ci-tools makefile changes between the two pins (from a direct diff): test running now
  goes through `duckdb/scripts/ci/run_tests.py` when present (it is, in 2.0),
  `TEST_PATH`/`TESTS_BASE_DIRECTORY` de-quoted, and new opt-in variables
  (`DUCKDB_NEW_EXTENSION_BUILD`, `DUCKDB_PREBUILT_LIBRARY`, `BUILD_EXTENSIONS_ONLY`) that
  capped does not set. No action needed; `make test` still means
  `run_tests.py ./build/release/test/unittest test/*` against capped's `test/` dir.

## 6. C2 checklist (do in this order)

Status after C2 (2026-11-12): ✅ done, ⚠️ done with a correction noted, details in §8.

1. ✅ Bump submodules in `capped/shell`: duckdb → `v2.0.0`, extension-ci-tools →
   `v2.0-cyanoptera`. Nothing else in CI/Makefiles/CMake needs touching (§5) — true, no
   file changed there; but see §8 for the two build-invocation env vars that are
   required (`OVERRIDE_GIT_DESCRIBE`, `DUCKDB_TEST_CONFIG`). ⚠️ ci-tools target is a
   branch, not a tag (§5).
2. ✅ Group 4 first (unblocks the header): 2-arg `TryLookupEntryInternal` in
   `capped_catalog.hpp:100-101` + `capped_catalog.cpp:176` (§3.4) — kills the error that
   repeats in all 7 files.
3. ✅ Group 5: add `GetColumns()` override to `CappedTableEntry` (§3.5).
4. ✅ Group 8: add the `physical_plan_generator.hpp` include (§3.8).
5. ✅ Group 1: the `Identifier` sweep — `Identifier(...)` inputs and
   `.GetIdentifierName()` outputs at the sites in §3.1, plus the bind-callback signatures.
6. ✅ Group 6: parsed-data fields (`schema_info.schema` line delete; `base.table` →
   `GetTableName()`; `info.name` → `GetQualifiedName().Name()`; 3-arg `CreateTableInfo` →
   `QualifiedName` ctor) (§3.6).
7. ⚠️ Group 2 + 3: function signatures — the §3.2 pattern as originally written is wrong
   (keyword-only parameters without a default are *required* to the 2.0 binder); fixed
   per the corrected §3.2, and the manual `TableFunctionBindInput` call in
   `capped_table.cpp:253-262` per §3.3 plus the `shared_ptr` overload copy (§3.3).
8. ✅ Group 7 + 9: `result.GetTypes()`; `ConstantExpression::GetLiteral().ToValue()` (§3.7,
   §3.9).
9. ✅ Deprecations in §3.10: mechanical ones applied; `DataChunk::SetValue` (55 sites)
   listed there for the owner.
10. ✅ `cd capped && cargo test` — 76 passed, clippy clean, Rust core untouched; the C
    ABI did not have to move.
11. ✅ `cd capped/shell && GEN=ninja make release && make test && make asan` — green
    with no extra environment variables (Makefile now defaults `OVERRIDE_GIT_DESCRIBE`
    and self-stages fts/vss, §8.1); `make asan` fully green, 2119 assertions, after
    adopting `detect_stack_use_after_return=1` (§8.6.3).
12. ✅ No test was edited (no skips, no weakening either). The failures found are
    classified in §8.6: two environmental (a) + one asan stack-capacity (a); one genuine
    port bug (signature defaults) was found and fixed.
13. C3 afterwards: lotel `duckdb = "=1.20000.1"`, same feature list, and re-point the
    extension paths at `~/.duckdb/extensions/v2.0.0/...` (both extensions verified above).
    Note for C3: build lotel's duckdb with `OVERRIDE_GIT_DESCRIBE=v2.0.0` too, or its
    dev-stamped build will reject extensions stamped `v2.0.0` (§8).

## 7. Unresolved / could not determine

- **sqllogictest outcome is unknown** until C2 compiles — no test can pre-validate the
  2.0 error-message and planner behaviour changes (see §4 risk).
- **Named-parameter binding semantics**: the signature-based declaration is verified
  present (`function.hpp:349`, glob/checkpoint/range precedents), but whether the 2.0
  binder accepts `k => 5` exactly like 1.5's `named_parameters` map (type coercion,
  unknown-name errors) can only be confirmed by running `capped_search.test` after C2 step
  7. The otlp precedent (`input.named_parameters` reads work on 2.0) is encouraging, not
  conclusive.
- **No v2.0.1 tag yet** — if one appears during C2, re-run §1a/§1b before committing; the
  crate patch line would need the matching `1.20000.2`-style release (none exists as of
  this survey).
- The trial build ran on macOS/arm64 only; CI (linux/amd64, vcpkg-free) was not exercised
  locally.

## 8. C2 result (executed 2026-11-12, macOS/arm64)

**Verdict: port complete.** Full build clean, `make test` 7/7 files green, `make asan`
7/7 green (2119 assertions; one macOS ASAN stack-capacity wrinkle in DuckDB 2.0 core
solved via `detect_stack_use_after_return=1` — §8.6.3), Rust core and C ABI untouched,
`git -C duckdb describe --tags` → `v2.0.0`. No test was edited; the Makefile now
makes the build and tests runnable with **no extra environment variables** (§8.1) —
README documents it.

### 8.1 Deviations / build-invocation requirements

**Follow-up (2026-11-12): items 3 and 4 are now built into `Makefile` (repo root) — the
build and tests need no extra environment variables at all** (`env -i` with only
`PATH`+`HOME` and `GEN=ninja make release && make test` passes; documented in the
repo README's Building section). Kept here as the rationale:

1. **extension-ci-tools `v2.0-cyanoptera` is a branch, not a tag** (§5). Checked out at
   `c286b12` via `git fetch --depth 1 origin v2.0-cyanoptera && git checkout -B
   v2.0-cyanoptera FETCH_HEAD`.
2. **Stale build caches wiped:** `shell/build/release` and `shell/build/asan` carried
   1.5.6-era CMake caches (`CMAKE_CXX_STANDARD=11`), which duckdb 2.0's
   `CMakeLists.txt:63` rejects outright. Both directories deleted before configuring —
   build artifacts only, no source change.
3. **`OVERRIDE_GIT_DESCRIBE=v2.0.0` must be set for the build** — now
   `OVERRIDE_GIT_DESCRIBE ?= v2.0.0` in `Makefile` (repo root). Without it the shallow duckdb
   submodule makes 2.0's CMake compute `v2.0.0-dev1` with normalized version = the
   commit hash (`CMakeLists.txt:564-578`: dev iteration = `git rev-list --count HEAD` =
   1 on a shallow clone). Consequences of the dev stamp: `capped.duckdb_extension` is
   stamped `d54cbbe3df`, and the release-built official extensions (`fts`, `vss`) refuse
   to load ("built specifically for DuckDB version 'v2.0.0'"). DuckDB's own CI does the
   same for shallow checkouts. With it: `duckdb --version` → `v2.0.0 (Cyanoptera)
   d54cbbe3df`, the local extension repository becomes
   `build/release/repository/v2.0.0/osx_arm64/`, and `capped.duckdb_extension` carries
   the release stamp C3/lotel will need.
4. **fts/vss staging for the two search tests** (§8.6) — the `remote-extensions`
   Makefile target now does by hand what C2 did manually: one `curl
   http://extensions.duckdb.org/v2.0.0/<platform>/{fts,vss}.duckdb_extension.gz | gunzip`
   per extension into `build/remote-extensions/v2.0.0/<platform>/` (cached across
   runs), plus a generated `test-config.json`; `test_release`/`test_debug`/
   `test_reldebug`/`test_search`/`asan` depend on it and run with
   `DUCKDB_TEST_CONFIG` exported to that config, which sets the `extension_directory`
   DB setting — the supported hook, because the 2.0 test harness replaces `HOME` with a
   fresh per-run temp dir (`unittest.cpp:462-478`) while `ENABLE_EXTENSION_AUTOINSTALL=0`
   blocks downloads.

Reproduce the green run from the repo root (`capped/`):

```sh
env -i PATH=/opt/homebrew/bin:/usr/bin:/bin HOME=$HOME sh -c 'GEN=ninja make release && make test'
make asan    # green too — see 8.6.3 (detect_stack_use_after_return=1)
```

(`run_tests.py` needs `python3` >= 3.10 on `PATH` — macOS' `/usr/bin/python3` is too
old, so homebrew's bin must precede `/usr/bin`.)

### 8.2 Compile-error burn-down (group order from §6)

Unique `file:line: error:` lines after each group, from `ninja -k 0` (static + loadable
TUs deduped):

| Step | Remaining unique errors |
|---|---|
| baseline (fresh 2.0 configure, before any fix) | 162 raw `error:` lines, 7 capped TUs |
| §6.2 Group 4 (`TryLookupEntryInternal` 2-arg) | 48 |
| §6.3 Group 5 (`GetColumns()` override) | 47 |
| §6.4 Group 8 (`physical_plan_generator.hpp` include) | 45 |
| §6.5 Group 1 (Identifier sweep) | 13 |
| §6.7 partial (`shared_ptr` overload copy, §3.3) | 12 |
| §6.6 Group 6 (parsed-data / `QualifiedName` ctors) | 10 |
| §6.7 Groups 2+3 (signatures + `BoundTableFunction`) | 2 |
| §6.8 Groups 7+9 (`GetTypes`, `GetLiteral`) | **0** (ninja exit 0) |

### 8.3 Fixes not in §3 (new in 2.0, discovered during C2)

- `FunctionSet<T>::functions` is now `vector<shared_ptr<const T>>`
  (`function/function_set_base.hpp:28-30`) → `LookupParquetScan` must return
  `*overloads[0]`, not `overloads[0]` (added to §3.3).
- Optional function parameters need explicit defaults in their `FunctionSignature`;
  keyword-only without a default is *required* (`GetRequiredParameterCount`). `k` must
  be a STANDARD parameter (positional or named); `rebuild`/`vector_column` keyword-only
  with defaults. Added to §3.2. The dual `fts_k`/`vec_k` overloads were deleted — each
  function is now a single signature. 1.x call-parity was verified with a scratch
  sqllogictest (removed afterwards, suite untouched): 4-positional `capped_fts(…, 1)`
  → k honoured (not shadowed by the default), named `k := 1` honoured,
  `capped_vector_search(…, 1)` positional k honoured, and `capped_index(c, t, true)`
  correctly rejected (rebuild is name-only, as in 1.x).

### 8.4 Deprecations

See §3.10: 11× `SetCardinality`→`SetCardinalityUnsafe` (verified byte-identical to the
deprecated wrapper) and the `EntryLookupInfo` `QualifiedName` fix applied; 55
`DataChunk::SetValue` sites left as warnings for the owner (needs per-site
`Vector::Append`/`SetValue` restructure — not one-line mechanical).

### 8.5 Rust core

Untouched; C ABI unchanged. `cargo test`: **76 passed, 0 failed** (+doc-tests).
`cargo clippy --all-targets -- -D warnings`: clean.

### 8.6 sqllogictests — every failure, expected vs actual, classified

Full suite (`run_tests.py ./build/release/test/unittest test/*`), no test edited, no
test skipped. First run (plain `make test`, no staging): **5 passed, 2 failed**.

1. `test/sql/capped_search.test:15` / `test/sql/capped_fragments_e2e.test:13` —
   `statement ok: LOAD fts` (and `LOAD vss`) failed. Actual:
   `IO Error: Extension "<temp-home>/.duckdb/extensions/d54cbbe3df/osx_arm64/fts.duckdb_extension"
   not found. Extension "fts" is an existing extension. Install it first using
   "INSTALL fts".` — **class (a)**: DuckDB 2.0 harness behaviour (per-run `HOME`
   isolation, `unittest.cpp:462`; strict extension version check in release builds) and
   fts/vss being remote-only extensions. Not a capped bug; the test's own header
   anticipates "fail loudly" without them. Resolved environment-side per §8.1.4 +
   §8.1.3 (no test or Makefile change): both files then run for real
   (`capped_search.test`: 88 assertions).
2. After unblocking the LOADs, one genuine failure appeared — **class (b), port bug,
   fixed**: `test/sql/capped_search.test:84`
   `SELECT * FROM capped_fts('ss', 'slogs', 'timed')` expected the
   `no searchable sealed segments` error path, actual:
   `Binder Error: No function matches the given name and argument types
   'capped_fts(STRING_LITERAL, STRING_LITERAL, STRING_LITERAL)'` (candidates listed
   `k BIGINT` as required). Cause: my §3.2 group-6 registrations made `k` a keyword-only
   parameter **without a default**, which 2.0's
   `FunctionSignature::GetRequiredParameterCount` counts as required — the 3-positional-arg
   overload never matched. Fixed with signature defaults (§8.3); same for `rebuild` and
   `vector_column`.

Final state: `make test` → **ran tests: 7 passed, 0 skipped** (all files, incl.
`capped_search.test` 88 assertions, `capped_fragments_e2e.test`). No DuckDB-2.0
error-message drift showed up in the other five test files — §4's risk did not
materialise.

3. `make asan` (the M17 memory-safety gate) — **class (a), left for the owner**:
   `test/sql/capped_fragments_e2e.test` dies at its first search statement with

   ```
   ==83189==ERROR: AddressSanitizer: stack-overflow ... T133
   SUMMARY: AddressSanitizer: stack-overflow (capped.duckdb_extension ...)
   ```

   Stack trace (truncated at frame 117): the repeating cycle
   `RelationManager::ExtractJoinRelations → QueryGraphManager::Build →
   JoinOrderOptimizer::Optimize` (duckdb **core** join-order optimiser) running under
   `Pipeline::ResetSource → PhysicalTableScan::GetGlobalSourceState → FtsInitGlobal →
   CappedIndex::SearchText → ForEachSidecarRow → Connection::Query` — i.e. duckdb
   binding capped's per-segment sidecar query (`capped_index.cpp:527-530`, a nested
   `SELECT ... FROM (SELECT ... fts_main_<t>.match_bm25(...)) ORDER BY score DESC
   LIMIT k`, unchanged by the port — `capped_index.cpp` is not in the changed-files
   list) on a duckdb worker thread, whose default macOS stack is 512 KB, while ASAN
   inflates every frame. All 112 preceding e2e assertions pass; the same file passes
   in the release suite; `capped_search.test`'s (smaller) sidecar queries fit even
   under ASAN. Nothing on the capped side moved: same query text, same call depth as
   1.5.6 — 2.0's optimiser simply recurses deeper per relation (extra
   `QueryGraphManager::Build`/`Optimize` frames per level) and no longer fits in
   512 KB once instrumented. **Per-file asan status** (test-config with
   `extension_directory` + `threads=1`): `capped.test` ✓, `capped_search.test` ✓ (88
   assertions), `capped_fragments.test` ✓ (1455), `capped_maintenance.test` ✓ (130),
   `capped_observability.test` ✓ (48), `capped_concurrent_tx.test` ✓ (61) — all with 0
   AddressSanitizer errors; only `capped_fragments_e2e.test` ✗ (the overflow above).
   Mitigations tried before the fix: test-config
   `threads=1` (helps the top-level search file — it then passes — but the e2e nested
   bind still runs on a scheduler thread); `--thread-stack-size` exists in 2.0's
   unittest but is **Linux/glibc-only** (`unittest.cpp:284`); no DB setting for
   worker stack size exists in 2.0.

   **RESOLVED (follow-up, same day) — owner asked for two cheap experiments:**
   - **(a) `ASAN_OPTIONS=detect_stack_use_after_return=1` — WORKS.** ASAN fake
     stacks move instrumented frame locals to the heap, shrinking the on-stack frame
     so 2.0's deeper core recursion fits the 512 KB worker stack:
     `capped_fragments_e2e.test` passes standalone (202 assertions) and the full
     gate is green — `All tests passed (2119 assertions in 7 test cases)`, `make
     asan` exit 0. **Adopted:** the option is now in the `asan` target's
     ASAN_OPTIONS in `Makefile` (repo root, with a comment), so no test or artifact
     changed and macOS runs the gate too.
   - **(b) asan build at `-O1` — DOES NOT HELP.** A separate instrumented tree
     (`build/asan-o1`, `-DCMAKE_{C,CXX}_FLAGS_RELEASE="-O1 -DNDEBUG"`, plain ASAN
     options) dies with the byte-identical overflow (same SUMMARY frame, 112 of 113
     assertions in) — per-frame inlining is not the driver; the recursion depth
     itself is. `-O1` was not adopted; the shipped asan target keeps the normal
     Release flags plus option (a).

   The classification stands: it was a DuckDB 2.0 core behaviour change (recursion
   depth) hitting a macOS thread-stack default under ASAN, not a capped bug; linux
   (8 MB pthread stacks) never hits it either way.

### 8.7 Checks tail

- `cd capped && cargo test` → `test result: ok. 76 passed; 0 failed` + doc-tests ok.
- `cargo clippy --all-targets -- -D warnings` → Finished, no warnings.
- `cd capped/shell && GEN=ninja make release` (no extra env) → exit 0; artifacts
  `build/release/extension/capped/capped.duckdb_extension`,
  `build/release/test/unittest`; `duckdb --version` → `v2.0.0 (Cyanoptera) d54cbbe3df`.
- `make test` (no extra env; Makefile stages fts/vss itself) → `ran tests: 7 passed,
  0 skipped in 8s`. Same via `env -i` with only `PATH`+`HOME` (README).
- `make asan` (Makefile as shipped) → `All tests passed (2119 assertions in 7 test
  cases)`, exit 0 — with `detect_stack_use_after_return=1` in the target's
  ASAN_OPTIONS (§8.6.3). Without it, `capped_fragments_e2e.test` overflows the macOS
  worker stack in core's join-order optimiser (class (a)); the other six files pass
  either way with 0 AddressSanitizer errors.
- `git -C duckdb describe --tags` → `v2.0.0`;
  `git -C extension-ci-tools log -1 --format='%h %d'` → `c286b12 (HEAD ->
  v2.0-cyanoptera)`.

### 8.8 Files changed (all uncommitted, left in working tree)

Submodules: `shell/duckdb` → `d54cbbe3df` (v2.0.0), `shell/extension-ci-tools` →
`c286b12` (branch `v2.0-cyanoptera`). Sources (8 files):
`capped_catalog.cpp` + `include/capped_catalog.hpp`, `capped_extension.cpp`,
`capped_schema.cpp`, `capped_table.cpp`, `capped_transaction.cpp`, `capped_write.cpp`,
`include/capped_table.hpp` (+118/−92 lines). Follow-up (env-free build, owner-requested):
`shell/Makefile` (`OVERRIDE_GIT_DESCRIBE ?= v2.0.0`, `remote-extensions` staging target
wired into `test_*`/`test_search`/`asan`, `detect_stack_use_after_return=1` in the asan
options) and `README.md` (Building section, v2.0.0 in the module table). No
CMake/workflow/test changes. Untracked: this doc.

### 8.9 For C3

Build lotel's duckdb from the same commit **with `OVERRIDE_GIT_DESCRIBE=v2.0.0`** as
well: the version check compares stamps, so a dev-stamped lotel build (`d54cbbe3df`)
would reject this `v2.0.0`-stamped `capped.duckdb_extension`, and vice versa. The
`~/.duckdb/extensions/v2.0.0/osx_arm64/` paths §6.13 predicts for C3 then line up with
the release CLI as well.
