# Name check before publishing (Job E, task E1)

Check date: 2026-11-16. Read-only check: nothing under `capped/` was changed except this file.
Closes the README's open item "name revisit before publishing" (`README.md`, M2c line).

Names checked: `capped` (current) plus three alternatives — `sliding`, `retention`, `shortmem`
— against four sources each:

1. DuckDB community extension list — <https://duckdb.org/community_extensions/list_of_extensions.html>
   (lists extensions for the latest stable release, v2.0.0; page fetched in full, 434 lines)
2. `duckdb/community-extensions` repo `extensions/` folder — fetched via
   <https://api.github.com/repos/duckdb/community-extensions/contents/extensions>
   (437 directory entries, one per extension; below GitHub's 1000-entry page limit, so the
   listing is complete; this is also the list that includes legacy extensions)
3. DuckDB core extension names — <https://duckdb.org/docs/2.0/core_extensions/overview.html>
   (32 entries; their aliases `http`/`https`/`s3`, `mysql_scanner`, `odbc_scanner`,
   `sqlite_scanner`/`sqlite3`, `md`, `uc_catalog` also carry no collision)
4. crates.io — `cargo search <name>` from the workspace plus an exact-match lookup via the
   crates.io API (`https://crates.io/api/v1/crates/<name>`, the data behind
   `https://crates.io/crates/<name>`), for both `<name>` and `<name>-core`
5. An obvious web search (`"<name>" duckdb extension …`)

Registry note: the community-registry name must be unique and lowercase (`[a-z_]+`); all four
candidates are valid format-wise.

## `capped` (current name)

| Place | Checked via | What it returned | `capped` free? |
| --- | --- | --- | --- |
| Community extension list | `list_of_extensions.html` fetch + case-insensitive grep for `capped` | 0 matches; the `c` block runs cache_httpfs, cache_prewarm, capi_quack, cardinal, cassandra, … — no capped | yes |
| community-extensions `extensions/` | GitHub API folder listing (437 entries) + grep | 0 directories matching `capped` | yes |
| DuckDB core extensions | core extensions overview table (32 names + aliases) | no capped | yes |
| crates.io `capped` | `cargo search capped`; `api/v1/crates/capped` | `capped = "0.0.1"` exists — "Provides wrappers around common rust types with capped sizes", RuairidhWilliamson/capped, MIT OR Apache-2.0, published 2024-08-03, 1 version, ~4.0k downloads | **taken** |
| crates.io `capped-core` | `cargo search capped-core`; `api/v1/crates/capped-core` | search returned no rows; API → 404 not found | yes |
| Web search | `"capped" duckdb extension storage` | community list, extension-hosting blogs; no `duckdb-capped` project or anything confusing | yes |

Verdict: free everywhere the extension name itself lives (both DuckDB registries, and the
GitHub repo name `duckdb-capped` per the web search). The one hard collision is the unrelated
`capped` crate on crates.io. This project publishes `capped-core` (the only workspace member;
`crates/capped-core/Cargo.toml`), which is free — but a crate literally named `capped` could
never be published here.

## Alternative 1: `sliding`

(sliding-window retention — what the store actually does)

| Place | Checked via | What it returned | `sliding` free? |
| --- | --- | --- | --- |
| Community extension list | fetch + grep | 0 matches | yes |
| community-extensions `extensions/` | GitHub API listing + grep | 0 matches | yes |
| DuckDB core extensions | overview table | no sliding | yes |
| crates.io `sliding` | `cargo search sliding`; `api/v1/crates/sliding` | search top hits are unrelated (`slide-core`, `slide-theme`, `slide-player`, `ohp`, `ppt-rs`), no exact `sliding`; API → 404 | yes |
| crates.io `sliding-core` | `cargo search sliding-core`; `api/v1/crates/sliding-core` | search returned no rows; API → 404 | yes |
| Web search | `"sliding" duckdb extension` | general DuckDB extension docs/blogs; no extension named sliding | yes |

## Alternative 2: `retention`

(the feature word itself)

| Place | Checked via | What it returned | `retention` free? |
| --- | --- | --- | --- |
| Community extension list | fetch + grep | only match is the word inside the `behavioral` extension's description; no extension named retention | yes |
| community-extensions `extensions/` | GitHub API listing + grep | 0 matches | yes |
| DuckDB core extensions | overview table | no retention | yes |
| crates.io `retention` | `cargo search retention`; `api/v1/crates/retention` | search top hits unrelated (`esuiss-retention`, `timberfs`, …), no exact `retention`; API → 404 | yes |
| crates.io `retention-core` | `cargo search retention-core`; API | search returned no rows; API → 404 | yes |
| Web search | `"retention" duckdb extension storage` | generic data-retention material (Salesforce Marketing Cloud "Data Retention for Data Extensions", SAP NSE, Splunk entitlements); no DuckDB extension named retention | yes, but the term is generic — heavy search noise and a realistic future collision candidate in the registry |

## Alternative 3: `shortmem`

(short-term memory: data that ages out by design)

| Place | Checked via | What it returned | `shortmem` free? |
| --- | --- | --- | --- |
| Community extension list | fetch + grep | 0 matches | yes |
| community-extensions `extensions/` | GitHub API listing + grep | 0 matches | yes |
| DuckDB core extensions | overview table | no shortmem | yes |
| crates.io `shortmem` | `cargo search shortmem`; `api/v1/crates/shortmem` | search returned no rows; API → 404 | yes |
| crates.io `shortmem-core` | `cargo search shortmem-core`; API | search returned no rows; API → 404 | yes |
| Web search | `"shortmem" database storage extension retention` | no relevant projects | yes |

## Alternatives rejected during screening

Checked only as far as it took to eliminate them (crates.io exact-match lookup):

| Name | Eliminator |
| --- | --- |
| `retained` | crates.io: `retained = "0.4.0"` — "Retain local variables between function calls" (`cargo search retained`) |
| `tide` | crates.io: `tide = "0.17.0-beta.1"` — the well-known web framework, ~3.7M downloads (API) |
| `evergreen` | crates.io: `evergreen = "0.2.0-alpha1"` (libp2p network backend) **and** `evergreen-core = "0.1.0"` (API) |
| `mayfly` | crates.io: `mayfly = "0.0.1"` name-reservation crate (API); `mayfly-core` is free but the root name is not |

## Recommendation

Keep **capped**. The name is free in both places the extension identity actually lives — the
community-extensions registry (list page and `extensions/` folder) and the core extension
name space — and the crates.io collision is on `capped`, a single-version, low-adoption,
unrelated utility crate that this project never publishes: the workspace crate is
`capped-core`, which is free, and `INSTALL capped FROM community` / the `duckdb-capped` repo
name are both unclaimed. The only cost of keeping it is that a crate literally named `capped`
is off the table (and `cargo add capped` lands someone on an unrelated crate — a discoverability
speed bump, not a blocker). If a full rename ever becomes necessary — e.g. we later want to
publish a facade crate named `capped`, or a squatter escalates — `sliding` is the strongest
drop-in (free at every source checked, and describes the sliding-window retention directly),
with `shortmem` as the runner-up; `retention` is free today too, but as a bare generic word it
carries the highest future-collision and SEO noise risk of the three. Re-run this check against
`duckdb/community-extensions` immediately before opening the `description.yml` PR, since the
registry list moves weekly.
