cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME capped)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD
    "17"
    CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)

set(EXTENSION_SOURCES
    src/capped_extension.cpp
    src/capped_catalog.cpp
    src/capped_index.cpp
    src/capped_schema.cpp
    src/capped_table.cpp
    src/capped_storage_info.cpp
    src/capped_transaction.cpp
    src/capped_write.cpp)

# --- Rust core (crates/capped-core, built as a staticlib) --------------------
# All retention logic lives in Rust; the shell links its C ABI. The crate-type
# staticlib lives in crates/capped-core/Cargo.toml.
set(CAPPED_WORKSPACE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(CAPPED_CORE_TARGET_DIR ${CAPPED_WORKSPACE_DIR}/target/capi)

# The CI osx_amd64 job cross-compiles on arm64 runners: DuckDB passes
# -DOSX_BUILD_ARCH=x86_64 and targets clang at x86_64, but cargo builds the
# host arch unless told, and linking an arm64 staticlib into a x86_64 dylib
# fails. ci-tools' macOS setup runs `rustup target add x86_64-apple-darwin`
# (extra_toolchains: rust) exactly for this cross-build.
if("${OSX_BUILD_ARCH}" STREQUAL "x86_64" OR "${CMAKE_OSX_ARCHITECTURES}" STREQUAL "x86_64" OR "$ENV{OSX_BUILD_ARCH}" STREQUAL "x86_64")
  set(CAPPED_RUST_TARGET x86_64-apple-darwin)
  set(CAPPED_RUST_TARGET_ARGS --target ${CAPPED_RUST_TARGET})
  set(CAPPED_CORE_PROFILE_DIR ${CAPPED_CORE_TARGET_DIR}/${CAPPED_RUST_TARGET}/release)
else()
  # Empty *list*, not an empty string: VERBATIM would pass "" as an argv to cargo.
  set(CAPPED_RUST_TARGET_ARGS)
  set(CAPPED_CORE_PROFILE_DIR ${CAPPED_CORE_TARGET_DIR}/release)
endif()
set(CAPPED_CORE_LIB ${CAPPED_CORE_PROFILE_DIR}/libcapped_core.a)

# CONFIGURE_DEPENDS re-globs at build time so new .rs files are picked up
file(GLOB_RECURSE CAPPED_CORE_SOURCES CONFIGURE_DEPENDS
     ${CAPPED_WORKSPACE_DIR}/crates/capped-core/src/*.rs)

add_custom_command(
  OUTPUT ${CAPPED_CORE_LIB}
  COMMAND cargo build -p capped-core --release ${CAPPED_RUST_TARGET_ARGS} --target-dir ${CAPPED_CORE_TARGET_DIR}
  DEPENDS ${CAPPED_CORE_SOURCES}
          ${CAPPED_WORKSPACE_DIR}/crates/capped-core/Cargo.toml
          ${CAPPED_WORKSPACE_DIR}/Cargo.lock
  WORKING_DIRECTORY ${CAPPED_WORKSPACE_DIR}
  COMMENT "Building capped-core (Rust staticlib)"
  VERBATIM)
add_custom_target(capped_core ALL DEPENDS ${CAPPED_CORE_LIB})

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

add_dependencies(${EXTENSION_NAME} capped_core)
add_dependencies(${LOADABLE_EXTENSION_NAME} capped_core)
target_link_libraries(${EXTENSION_NAME} ${CAPPED_CORE_LIB})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${CAPPED_CORE_LIB})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
