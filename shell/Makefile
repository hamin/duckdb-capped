PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=capped
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# DuckDB 2.0 stamps a shallow-checkout submodule build as v2.0.0-dev<commits>,
# using the commit hash as its extension version — official v2.0.0 extensions
# (the fts/vss the search tests LOAD) then refuse to load, and capped's own
# .duckdb_extension gets a hash stamp too. Pin the describe output so the
# build behaves like the v2.0.0 release (what the submodules are checked out
# at). Override for off-tag builds: `OVERRIDE_GIT_DESCRIBE=... make ...`.
# (docs/duckdb-2.0-port.md §8.1.3)
OVERRIDE_GIT_DESCRIBE ?= v2.0.0

# The search tests LOAD fts/vss, which are remote extensions. The 2.0 test
# harness swaps HOME for a fresh per-run temp dir, so ~/.duckdb is invisible
# to LOAD; the supported hook is a test config (DUCKDB_TEST_CONFIG) pointing
# the `extension_directory` setting at a local stage. `make test`,
# `make test_search` and `make asan` stage the official builds for
# $(OVERRIDE_GIT_DESCRIBE) from extensions.duckdb.org into
# build/remote-extensions themselves (one curl per extension, kept across
# runs). (docs/duckdb-2.0-port.md §8.1.4)
REMOTE_EXT_PLATFORM ?= $(shell uname -s | tr '[:upper:]' '[:lower:]' | sed 's/darwin/osx/')_$(shell uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
REMOTE_EXT_STAGE := $(PROJ_DIR)build/remote-extensions
REMOTE_EXT_LIST := fts vss

$(REMOTE_EXT_STAGE)/$(OVERRIDE_GIT_DESCRIBE)/$(REMOTE_EXT_PLATFORM)/%.duckdb_extension:
	@mkdir -p $(@D)
	@echo "==> staging official $* $(OVERRIDE_GIT_DESCRIBE) for $(REMOTE_EXT_PLATFORM)"
	@curl -fsSL http://extensions.duckdb.org/$(OVERRIDE_GIT_DESCRIBE)/$(REMOTE_EXT_PLATFORM)/$*.duckdb_extension.gz | gunzip > $@

$(REMOTE_EXT_STAGE)/test-config.json:
	@mkdir -p $(@D)
	@printf '{"settings": [{"name": "extension_directory", "value": "%s"}]\n}' "$(REMOTE_EXT_STAGE)" > $@

.PHONY: remote-extensions
remote-extensions: $(REMOTE_EXT_STAGE)/test-config.json $(addprefix $(REMOTE_EXT_STAGE)/$(OVERRIDE_GIT_DESCRIBE)/$(REMOTE_EXT_PLATFORM)/,$(addsuffix .duckdb_extension,$(REMOTE_EXT_LIST)))

export DUCKDB_TEST_CONFIG := $(REMOTE_EXT_STAGE)/test-config.json

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# The search suite drives real fts (BM25) and vss (HNSW) indexes, which the test
# file LOADs itself; run it after `make release`.
test_search: remote-extensions
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
#   detect_stack_use_after_return=1  ASAN moves instrumented frame locals to the
#       heap. Needed on macOS: duckdb 2.0's deeper join-order-optimiser recursion
#       (core, not capped) overflows the 512 KB default worker-thread stack under
#       plain ASAN frames — see docs/duckdb-2.0-port.md §8.6.3.

# Gate the leaf targets that launch unittest (not `test` itself — its prereq
# order would put the runner before the staging).
test_release: remote-extensions
test_debug: remote-extensions
test_reldebug: remote-extensions
asan: remote-extensions
asan:
	mkdir -p build/asan
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_RELEASE_FLAGS) $(VCPKG_MANIFEST_FLAGS) -DCMAKE_BUILD_TYPE=Release -DRELEASE_SANITIZER=1 -S $(DUCKDB_SRCDIR) -B build/asan
	cmake --build build/asan --config Release
	ASAN_OPTIONS=detect_leaks=0:detect_container_overflow=0:abort_on_error=1:detect_stack_use_after_return=1 build/asan/test/unittest "test/*"
