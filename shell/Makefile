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

# Memory-safety gate (M17): build duckdb + this extension under AddressSanitizer
# and run the whole sqllogictest suite inside it. Release mode with
# RELEASE_SANITIZER=1 is deliberate: it injects -fsanitize=address into the
# global compile flags, so shell/src/*.cpp is instrumented too, not just duckdb
# proper (CXX_EXTRA_DEBUG alone would miss the extension sources). ASAN errors
# abort the run, making this a gate rather than a log to read.
#
# ASAN_OPTIONS notes:
#   detect_leaks=0            LeakSanitizer does not support macOS.
#   detect_container_overflow=0  DuckDB core itself trips this check copying a
#       std::vector<GenericSetting> registered by the parquet extension — the
#       read stays inside the libc++ container's own poisoned capacity, so the
#       only code that could fix it is upstream. Real bug classes (use-after-
#       free, heap/stack overflows, double free) remain fully armed.
asan:
	mkdir -p build/asan
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_RELEASE_FLAGS) $(VCPKG_MANIFEST_FLAGS) -DCMAKE_BUILD_TYPE=Release -DRELEASE_SANITIZER=1 -S $(DUCKDB_SRCDIR) -B build/asan
	cmake --build build/asan --config Release
	ASAN_OPTIONS=detect_leaks=0:detect_container_overflow=0:abort_on_error=1 build/asan/test/unittest "test/*"
