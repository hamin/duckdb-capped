#pragma once

#include "capped.h"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

#include <functional>

namespace duckdb {

class FileSystem;

//! Store state on disk: <root>/<table>/manifest.json (+ segment files).
//! The manifest JSON is written by capped-core (Rust); the shell only moves
//! bytes around here. File operations go through DuckDB's FileSystem (not
//! std::filesystem: extensions are pinned to C++11).
class CappedStorageInfo {
public:
	//! Background enforcement cadence. 0 disables the background thread
	//! (ATTACH / writes / capped_enforce still enforce).
	static int64_t DefaultEnforceIntervalMicros() {
		return 60LL * 1000 * 1000; // 60s
	}

	explicit CappedStorageInfo(string root) : root(std::move(root)) {
	}

	const string &Root() const {
		return root;
	}

	int64_t EnforceIntervalMicros() const {
		return enforce_interval_micros;
	}
	void SetEnforceIntervalMicros(int64_t micros) {
		enforce_interval_micros = micros;
	}

	string TablePath(const string &table) const;
	string ManifestPath(const string &table) const;

	//! Create the store root if missing.
	void EnsureRoot(FileSystem &fs) const;
	bool ManifestExists(FileSystem &fs, const string &table) const;
	string ReadManifest(FileSystem &fs, const string &table) const;
	void WriteManifest(FileSystem &fs, const string &table, const string &manifest_json) const;
	//! Remove a table's directory entirely (manifest + segments).
	void RemoveTable(FileSystem &fs, const string &table) const;
	//! Table names: directories under root that contain a manifest.json.
	vector<string> ListTables(FileSystem &fs) const;

	//! Absolute store root from an ATTACH path (expands ~ and makes absolute).
	static string ExpandRoot(FileSystem &fs, const string &path);

private:
	string root;
	int64_t enforce_interval_micros = DefaultEnforceIntervalMicros();
};

//! RAII wrapper for strings allocated by capped-core.
class CappedString {
public:
	CappedString() : ptr(nullptr) {
	}
	~CappedString() {
		capped_free_string(ptr);
	}
	CappedString(const CappedString &) = delete;
	CappedString &operator=(const CappedString &) = delete;

	char **Receive() {
		return &ptr;
	}
	string Str() const {
		return ptr ? string(ptr) : string();
	}

private:
	char *ptr;
};

//! Invoke an FFI function of shape fn(out, out_err), throwing on failure.
inline string CappedCall(const std::function<int32_t(char **, char **)> &fn, const string &what) {
	CappedString out, err;
	if (fn(out.Receive(), err.Receive()) != CAPPED_OK) {
		throw IOException("capped: %s failed: %s", what, err.Str());
	}
	return out.Str();
}

//! Minimal JSON string escaping (manifest contents are the only JSON we build here).
string CappedEscapeJson(const string &s);

} // namespace duckdb
