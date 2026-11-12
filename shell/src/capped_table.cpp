#include "capped_table.hpp"

#include "capped.h"
#include "capped_catalog.hpp"
#include "capped_transaction.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "yyjson.hpp"

namespace duckdb {

namespace {

//! Map manifest type strings back to LogicalType. The manifest is human
//! readable, so scalars are written out explicitly; anything fancier is a
//! clear error rather than a silent miss. (A general type-string parser can
//! replace this when composite types are needed.)
LogicalType CappedTypeFromString(const string &type_str) {
	auto upper = StringUtil::Upper(type_str);
	if (upper == "BOOLEAN" || upper == "BOOL") {
		return LogicalType::BOOLEAN;
	} else if (upper == "TINYINT") {
		return LogicalType::TINYINT;
	} else if (upper == "SMALLINT") {
		return LogicalType::SMALLINT;
	} else if (upper == "INTEGER" || upper == "INT") {
		return LogicalType::INTEGER;
	} else if (upper == "BIGINT") {
		return LogicalType::BIGINT;
	} else if (upper == "HUGEINT") {
		return LogicalType::HUGEINT;
	} else if (upper == "UTINYINT") {
		return LogicalType::UTINYINT;
	} else if (upper == "USMALLINT") {
		return LogicalType::USMALLINT;
	} else if (upper == "UINTEGER") {
		return LogicalType::UINTEGER;
	} else if (upper == "UBIGINT") {
		return LogicalType::UBIGINT;
	} else if (upper == "FLOAT" || upper == "REAL") {
		return LogicalType::FLOAT;
	} else if (upper == "DOUBLE") {
		return LogicalType::DOUBLE;
	} else if (upper == "DATE") {
		return LogicalType::DATE;
	} else if (upper == "TIME") {
		return LogicalType::TIME;
	} else if (upper == "TIMESTAMP") {
		return LogicalType::TIMESTAMP;
	} else if (upper == "TIMESTAMP_S") {
		return LogicalType::TIMESTAMP_S;
	} else if (upper == "TIMESTAMP_MS") {
		return LogicalType::TIMESTAMP_MS;
	} else if (upper == "TIMESTAMP_NS") {
		return LogicalType::TIMESTAMP_NS;
	} else if (upper == "TIMESTAMPTZ" || upper == "TIMESTAMP WITH TIME ZONE") {
		return LogicalType::TIMESTAMP_TZ;
	} else if (upper == "BLOB") {
		return LogicalType::BLOB;
	} else if (upper == "VARCHAR" || upper == "TEXT" || upper == "STRING") {
		return LogicalType::VARCHAR;
	} else if (upper == "UUID") {
		return LogicalType::UUID;
	} else if (upper == "INTERVAL") {
		return LogicalType::INTERVAL;
	}
	// arrays: FLOAT[8] is the shape a vector column is declared with (vss only
	// indexes fixed-size float arrays); the element type parses recursively so
	// INTEGER[3] works too, and FLOAT[] stays a plain list
	auto open = upper.find('[');
	if (open != string::npos && StringUtil::EndsWith(upper, "]")) {
		auto dims = upper.substr(open + 1, upper.size() - open - 2);
		StringUtil::Trim(dims);
		auto child = CappedTypeFromString(upper.substr(0, open));
		if (dims.empty()) {
			return LogicalType::LIST(child);
		}
		if (dims.find_first_not_of("0123456789") == string::npos) {
			return LogicalType::ARRAY(child, NumericCast<idx_t>(std::stoull(dims)));
		}
	}
	throw IOException("capped: unsupported manifest column type '%s'", type_str);
}

} // namespace

CappedTableEntry::CappedTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, unique_ptr<CreateTableInfo> info)
    : TableCatalogEntry(catalog, schema, *info), create_info(std::move(info)) {
}

unique_ptr<CreateTableInfo> CappedTableEntry::BuildCreateTableInfo(Catalog &catalog, const string &table,
                                                                   const string &manifest_json) {
	auto info = make_uniq<CreateTableInfo>(
	    QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), Identifier(table)));

	CappedString err;
	int64_t count = 0;
	if (capped_manifest_column_count(manifest_json.c_str(), &count, err.Receive()) != CAPPED_OK) {
		throw IOException("capped: reading manifest columns failed: %s", err.Str());
	}
	for (int64_t i = 0; i < count; i++) {
		CappedString name, type;
		if (capped_manifest_column(manifest_json.c_str(), i, name.Receive(), type.Receive(), err.Receive()) !=
		    CAPPED_OK) {
			throw IOException("capped: reading manifest column %lld failed: %s", i, err.Str());
		}
		if (name.Str() == CAPPED_INGEST_TS_COLUMN) {
			// store-maintained age column: present in the segment files and in
			// retention decisions, but not part of the table users see — that
			// keeps INSERT arity and SELECT * natural (no value to supply)
			continue;
		}
		info->columns.AddColumn(ColumnDefinition(Identifier(name.Str()), CappedTypeFromString(type.Str())));
	}
	return info;
}

// --- scan path (M1b) -------------------------------------------------------

namespace {

void CappedEmptyScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	output.SetCardinalityUnsafe(0);
}

//! Source for capped tables with no sealed segments yet. The binder takes
//! names/types from the table schema for catalog scans, so the function
//! itself does not need to declare a return type.
TableFunction CappedEmptyScanFunction() {
	TableFunction fn("capped_empty_scan", {}, CappedEmptyScan);
	fn.projection_pushdown = true; // plan_get requires it when virtual columns exist
	return fn;
}

//! Every data file of every segment from capped_manifest_list_segments output:
//! each segment's `files` list flattened in order. That list names the files a
//! scan reads — [path] for a sealed or legacy open segment, the fragment files
//! of a fragment-based open segment in insertion order, and empty for a fresh
//! open segment that has written nothing yet (its `path` is only the planned
//! sealed name, so it contributes no file). Paths are store-root-relative;
//! the caller resolves them.
vector<string> SegmentPaths(const string &segments_json) {
	vector<string> paths;
	auto *doc = duckdb_yyjson::yyjson_read(segments_json.c_str(), segments_json.size(), 0);
	if (!doc) {
		throw IOException("capped: cannot parse segment list from manifest");
	}
	auto *root = duckdb_yyjson::yyjson_doc_get_root(doc);
	if (duckdb_yyjson::yyjson_is_arr(root)) {
		duckdb_yyjson::yyjson_arr_iter iter;
		duckdb_yyjson::yyjson_arr_iter_init(root, &iter);
		while (auto *seg = duckdb_yyjson::yyjson_arr_iter_next(&iter)) {
			auto *files = duckdb_yyjson::yyjson_obj_get(seg, "files");
			if (files && duckdb_yyjson::yyjson_is_arr(files)) {
				duckdb_yyjson::yyjson_arr_iter file_iter;
				duckdb_yyjson::yyjson_arr_iter_init(files, &file_iter);
				while (auto *file = duckdb_yyjson::yyjson_arr_iter_next(&file_iter)) {
					if (duckdb_yyjson::yyjson_is_str(file)) {
						paths.push_back(duckdb_yyjson::yyjson_get_str(file));
					}
				}
			} else {
				// both halves of the extension ship as one binary, so a list
				// without "files" can only mean the two halves disagree
				throw InternalException("capped: segment entry in capped_manifest_list_segments output has no "
				                        "\"files\" array: %s",
				                        segments_json);
			}
		}
	}
	duckdb_yyjson::yyjson_doc_free(doc);
	return paths;
}

//! The parquet extension provides the actual reader; segment scans are
//! parquet scans over the sealed segment files.
TableFunction LookupParquetScan(ClientContext &context) {
	auto &sys = Catalog::GetSystemCatalog(context);
	auto lookup = [&](const string &fname) {
		return sys.GetEntry<TableFunctionCatalogEntry>(
		    context, QualifiedName(sys.GetName(), Identifier::DefaultSchema(), Identifier(fname)),
		    OnEntryNotFound::RETURN_NULL);
	};
	auto entry = lookup("parquet_scan");
	if (!entry) {
		// loadable-extension hosts may not have parquet loaded yet
		ExtensionHelper::TryAutoLoadExtension(context, "parquet");
		entry = lookup("parquet_scan");
	}
	if (!entry) {
		throw IOException("capped: scanning segment files requires the 'parquet' extension (TRY LOAD parquet)");
	}
	auto &overloads = entry->functions.functions;
	if (overloads.empty()) {
		throw IOException("capped: parquet_scan has no overloads registered");
	}
	// 2.0 stores overloads as shared_ptr<const TableFunction>; copy so the
	// caller can own a mutable function (it rewrites name/bind inputs)
	return *overloads[0];
}

} // namespace

TableFunction CappedTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                                const EntryLookupInfo &lookup_info) {
	auto &capped_catalog = catalog.Cast<CappedCatalog>();
	auto &storage = capped_catalog.Storage();
	auto &fs = capped_catalog.FS();
	// Read-your-writes across an uncommitted INSERT is not supported: refuse
	// loudly instead of silently omitting this transaction's buffered rows.
	auto txn = Transaction::TryGet(context, capped_catalog.GetAttached());
	if (txn) {
		auto &capped_txn = txn->Cast<CappedTransaction>();
		if (capped_txn.HasBufferFor(name.GetIdentifierName())) {
			throw NotImplementedException(
			    "capped: reading table '%s' inside the transaction that inserted into it is not supported "
			    "yet — COMMIT first",
			    name.GetIdentifierName());
		}
	}
	// read the manifest fresh on every bind: newly sealed segments become
	// visible to new queries without re-attaching
	auto manifest = storage.ReadManifest(fs, name.GetIdentifierName());

	CappedString err, segments_json;
	if (capped_manifest_list_segments(manifest.c_str(), segments_json.Receive(), err.Receive()) != CAPPED_OK) {
		throw IOException("capped: listing segments of '%s' failed: %s", name.GetIdentifierName(), err.Str());
	}
	auto paths = SegmentPaths(segments_json.Str());
	if (paths.empty()) {
		return CappedEmptyScanFunction();
	}

	// compose with the parquet scan: files = sealed segments. The binder takes
	// column names/types from the table schema (bind_basetableref.cpp), and
	// parquet's own callbacks on the copied function keep filter/limit
	// pushdown and row-group pruning working exactly like read_parquet.
	// Note: multi-file input is ONE LIST VARCHAR value — MultiFileBind reads
	// only inputs[0] and expands lists (same as read_parquet(['a','b'])).
	vector<Value> inputs;
	vector<Value> file_values;
	for (auto &path : paths) {
		file_values.emplace_back(storage.Root() + "/" + path);
	}
	inputs.push_back(Value::LIST(LogicalType::VARCHAR, std::move(file_values)));

	auto scan = LookupParquetScan(context);
	TableFunctionRef ref;
	named_argument_map_t named_parameters;
	vector<LogicalType> input_table_types;
	vector<Identifier> input_table_names;
	// 2.0: bind inputs refer to a BoundTableFunction, not the catalog TableFunction
	BoundTableFunction bound_scan(scan);
	TableFunctionBindInput bind_input(inputs, named_parameters, input_table_types, input_table_names, nullptr,
	                                  nullptr, bound_scan, ref);
	vector<LogicalType> scratch_types;
	vector<Identifier> scratch_names;
	if (!scan.bind) {
		throw IOException("capped: unexpected: parquet_scan has no bind function");
	}
	bind_data = scan.bind(context, bind_input, scratch_types, scratch_names);

	// Statement cache: verified empirically that prepared statements rebind
	// when the store changes (the binder registers a catalog read and the
	// statement cache invalidates on it), so preset bind_data does not go
	// stale across commits.
	scan.name = "capped_scan";
	return scan;
}

TableFunction CappedTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	EntryLookupInfo lookup_info(CatalogType::TABLE_ENTRY, QualifiedName(name));
	return GetScanFunction(context, bind_data, lookup_info);
}

TableStorageInfo CappedTableEntry::GetStorageInfo(ClientContext &context) {
	// empty for now: per-column segment stats arrive with the scan path (M1b)
	// and row/byte counts with the write path (M1c)
	return TableStorageInfo();
}

} // namespace duckdb
