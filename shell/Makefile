PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=capped
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# The search suite drives real fts (BM25) and vss (HNSW) indexes, which the test
# file LOADs itself; run it after `make release`.
test_search:
	build/release/test/unittest "test/sql/capped_search.test"
