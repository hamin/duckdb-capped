#include "capped_storage_info.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

namespace {

//! Collapse "." / ".." segments and trailing separators. DuckDB's FileSystem
//! has no path normalizer, and manifests/segments are referenced by these
//! paths on disk, so we keep them tidy ourselves. '/' is the only separator
//! we emit (DuckDB's local filesystem accepts it everywhere).
string NormalizePath(const string &path) {
	vector<string> parts;
	for (auto &piece : StringUtil::Split(path, '/')) {
		if (piece.empty() || piece == ".") {
			continue;
		}
		if (piece == "..") {
			if (!parts.empty()) {
				parts.pop_back();
			}
			continue;
		}
		parts.push_back(piece);
	}
	string out;
	for (auto &part : parts) {
		out += "/" + part;
	}
	return out.empty() ? string("/") : out;
}

} // namespace

string CappedStorageInfo::TablePath(const string &table) const {
	return root + "/" + table;
}

string CappedStorageInfo::ManifestPath(const string &table) const {
	return TablePath(table) + "/manifest.json";
}

void CappedStorageInfo::EnsureRoot(FileSystem &fs) const {
	if (!fs.DirectoryExists(root)) {
		fs.CreateDirectoriesRecursive(root);
	}
}

bool CappedStorageInfo::ManifestExists(FileSystem &fs, const string &table) const {
	return fs.FileExists(ManifestPath(table));
}

string CappedStorageInfo::ReadManifest(FileSystem &fs, const string &table) const {
	auto path = ManifestPath(table);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
	if (!handle) {
		throw IOException("capped: cannot read manifest '%s'", path);
	}
	auto size = fs.GetFileSize(*handle);
	if (size < 0) {
		throw IOException("capped: cannot size manifest '%s'", path);
	}
	string result;
	result.resize(static_cast<size_t>(size));
	if (size > 0) {
		fs.Read(*handle, &result[0], size, 0);
	}
	return result;
}

void CappedStorageInfo::WriteManifest(FileSystem &fs, const string &table, const string &manifest_json) const {
	auto dir = TablePath(table);
	if (!fs.DirectoryExists(dir)) {
		fs.CreateDirectoriesRecursive(dir);
	}
	auto path = ManifestPath(table);
	// write-then-rename keeps a crash from leaving a half-written manifest
	auto tmp = path + ".tmp";
	if (fs.FileExists(tmp)) {
		fs.RemoveFile(tmp);
	}
	{
		auto handle = fs.OpenFile(tmp, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
		fs.Write(*handle, (void *)manifest_json.c_str(), static_cast<int64_t>(manifest_json.size()), 0);
	}
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}
	fs.MoveFile(tmp, path);
}

void CappedStorageInfo::RemoveTable(FileSystem &fs, const string &table) const {
	auto dir = TablePath(table);
	if (fs.DirectoryExists(dir)) {
		fs.RemoveDirectory(dir); // recursive
	}
}

vector<string> CappedStorageInfo::ListTables(FileSystem &fs) const {
	// DuckDB's Glob only emits files for the last component, so we match the
	// manifests themselves and derive table names from the parent directory.
	static const string MANIFEST_SUFFIX = "/manifest.json";
	vector<string> tables;
	if (!fs.DirectoryExists(root)) {
		return tables;
	}
	for (auto &entry : fs.Glob(root + "/*" + MANIFEST_SUFFIX, nullptr)) {
		auto &path = entry.path;
		if (path.size() <= MANIFEST_SUFFIX.size()) {
			continue;
		}
		string dir = path.substr(0, path.size() - MANIFEST_SUFFIX.size());
		auto name = dir.substr(dir.find_last_of('/') + 1);
		if (name.empty() || name[0] == '.') {
			continue;
		}
		tables.push_back(name);
	}
	std::sort(tables.begin(), tables.end());
	return tables;
}

string CappedStorageInfo::ExpandRoot(FileSystem &fs, const string &path) {
	if (path.empty()) {
		throw IOException("capped: ATTACH requires a store directory path");
	}
	string expanded = fs.ExpandPath(path); // ~ expansion
	if (!fs.IsPathAbsolute(expanded)) {
		expanded = FileSystem::GetWorkingDirectory() + "/" + expanded;
	}
	return NormalizePath(expanded);
}

string CappedEscapeJson(const string &s) {
	string out;
	out.reserve(s.size() + 8);
	for (unsigned char c : s) {
		switch (c) {
		case '"':
			out += "\\\"";
			break;
		case '\\':
			out += "\\\\";
			break;
		case '\n':
			out += "\\n";
			break;
		case '\r':
			out += "\\r";
			break;
		case '\t':
			out += "\\t";
			break;
		default:
			if (c < 0x20) {
				out += StringUtil::Format("\\u%04x", c);
			} else {
				out += char(c);
			}
		}
	}
	return out;
}

} // namespace duckdb
