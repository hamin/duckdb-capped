# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(capped
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)

# Segment scans delegate to the parquet reader (M1b); statically linked so the
# unittest binary and the built shell can read segment files.
duckdb_extension_load(parquet)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
