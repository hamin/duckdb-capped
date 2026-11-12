#define DUCKDB_EXTENSION_MAIN

#include "capped_extension.hpp"

#include "capped.h"
#include "capped_catalog.hpp"
#include "capped_index.hpp"
#include "capped_transaction.hpp"
#include "capped_write.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"

#include "yyjson.hpp"

#include <chrono>

namespace duckdb {

namespace {

unique_ptr<Catalog> CappedAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                 AttachedDatabase &db, const string &name, AttachInfo &info, AttachOptions &options) {
	// the store is a local directory; use our own LocalFileSystem (the db-level
	// filesystem wrappers are read-only opener views)
	LocalFileSystem fs;
	auto root = CappedStorageInfo::ExpandRoot(fs, info.path);
	CappedStorageInfo storage(std::move(root));
	// ATTACH options: enforce_interval (background cadence; '0' disables).
	for (auto &entry : options.options) {
		auto key = StringUtil::Lower(entry.first);
		if (key == "enforce_interval") {
			CappedString err;
			int64_t micros = 0;
			auto text = entry.second.ToString();
			if (capped_parse_duration(text.c_str(), &micros, err.Receive()) != CAPPED_OK) {
				throw ParserException("ATTACH (TYPE capped): invalid enforce_interval '%s': %s", text, err.Str());
			}
			storage.SetEnforceIntervalMicros(micros);
		} else {
			throw ParserException("ATTACH (TYPE capped): unknown option '%s' (supported: enforce_interval)",
			                      entry.first);
		}
	}
	storage.EnsureRoot(fs);
	bool read_only = options.access_mode == AccessMode::READ_ONLY;
	auto catalog = make_uniq<CappedCatalog>(db, std::move(storage), read_only);
	// TTL on idle stores: apply retention at open time too, so an attach after
	// a quiet period reflects the policy. Best effort — never fail an ATTACH
	// over an enforcement hiccup (a corrupt manifest surfaces via queries).
	// A READ_ONLY attach writes nothing to the store: no migration, sweep,
	// GC or TTL eviction here, no background thread (CappedCatalog::Initialize
	// skips it), and every write path is guarded at its entry point as well.
	if (!read_only) {
		for (auto &table : catalog->Storage().ListTables(catalog->FS())) {
			try {
				// migrate a legacy open segment, sweep due deletes, collect
				// orphan crash leftovers (§3.7/§3.8); best effort like the
				// enforce pass below — ATTACH must never fail over it
				CappedWrite::AttachMaintenance(*catalog, table);
				CappedWrite::Enforce(*catalog, table);
			} catch (std::exception &) {
				// ignore; correctness does not depend on this pass
			}
		}
	}
	return catalog;
}

unique_ptr<TransactionManager> CappedCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                              AttachedDatabase &db, Catalog &catalog) {
	auto &capped_catalog = catalog.Cast<CappedCatalog>();
	return make_uniq<CappedTransactionManager>(db, capped_catalog.Storage());
}

void RegisterStorageExtension(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	auto extension = make_shared_ptr<StorageExtension>();
	extension->attach = CappedAttach;
	extension->create_transaction_manager = CappedCreateTransactionManager;
	StorageExtension::Register(config, "capped", extension);
}

void RegisterVersionFunction(ExtensionLoader &loader) {
	ScalarFunction version_fn("capped_version", {}, LogicalType::VARCHAR, [](DataChunk &args, ExpressionState &state,
	                                                                         Vector &result) {
		// no arguments => must return a CONSTANT_VECTOR; a debug duckdb asserts on this
		// when the call is constant-folded (a flat vector passes silently in release)
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::GetData<string_t>(result)[0] = StringVector::AddString(result, string(::capped_version()));
	});
	loader.RegisterFunction(version_fn);
}

// --- capped_add_segment: adopt a parquet file as a sealed segment ----------
// Bulk-ingest / debugging utility: COPY data out to a file inside the table
// directory, then register it. Stats (rows/bytes/event-time range) are
// computed here from the parquet metadata, so the manifest stays truthful for
// eviction planning. INSERT lands in M1c and will use the same manifest path.

struct CappedAddSegmentBindData : public TableFunctionData {
	string catalog_name;
	string table;
	string rel_path;
};

struct CappedAddSegmentGlobalState : public GlobalTableFunctionState {
	idx_t emitted = 0;
	bool Finished() const {
		return emitted > 0;
	}
};

unique_ptr<FunctionData> AddSegmentBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 3) {
		throw BinderException("capped_add_segment(catalog, table, path) takes three VARCHAR arguments");
	}
	auto bind_data = make_uniq<CappedAddSegmentBindData>();
	bind_data->catalog_name = input.inputs[0].DefaultCastAs(LogicalType::VARCHAR).ToString();
	bind_data->table = input.inputs[1].DefaultCastAs(LogicalType::VARCHAR).ToString();
	bind_data->rel_path = input.inputs[2].DefaultCastAs(LogicalType::VARCHAR).ToString();
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("capped_add_segment");
	return bind_data;
}

unique_ptr<GlobalTableFunctionState> AddSegmentInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<CappedAddSegmentGlobalState>();
}

int64_t NowMicros() {
	return std::chrono::duration_cast<std::chrono::microseconds>(
	           std::chrono::system_clock::now().time_since_epoch())
	    .count();
}

//! Resolve catalog + table dir (case-insensitively, like catalog lookups),
//! throwing binder-style errors for the given table function name.
CappedCatalog &ResolveCappedCatalog(ClientContext &context, const string &catalog_name, const string &fn_name) {
	auto &cat = Catalog::GetCatalog(context, catalog_name);
	if (cat.GetCatalogType() != "capped") {
		throw CatalogException("%s: '%s' is not a capped catalog", fn_name, catalog_name);
	}
	return cat.Cast<CappedCatalog>();
}

string ResolveCappedTable(CappedCatalog &capped_catalog, const string &table, const string &fn_name) {
	for (auto &candidate : capped_catalog.Storage().ListTables(capped_catalog.FS())) {
		if (StringUtil::CIEquals(candidate, table)) {
			return candidate;
		}
	}
	throw CatalogException("%s: table '%s' does not exist", fn_name, table);
}

void AddSegmentFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<CappedAddSegmentGlobalState>();
	if (gstate.Finished()) {
		output.SetCardinality(0);
		return;
	}
	gstate.emitted++;
	auto &bdata = data.bind_data->Cast<CappedAddSegmentBindData>();

	auto &capped_catalog = ResolveCappedCatalog(context, bdata.catalog_name, "capped_add_segment");
	auto &storage = capped_catalog.Storage();
	auto &fs = capped_catalog.FS();

	auto table_dir = ResolveCappedTable(capped_catalog, bdata.table, "capped_add_segment");
	if (capped_catalog.ReadOnly()) {
		throw IOException("capped_add_segment: the store is attached READ_ONLY; nothing can be adopted");
	}
	auto prefix = table_dir + "/";
	if (bdata.rel_path.compare(0, prefix.size(), prefix) != 0) {
		throw IOException("capped_add_segment: path '%s' must live inside the table directory '%s/'",
		                  bdata.rel_path, table_dir);
	}
	auto abs_path = storage.Root() + "/" + bdata.rel_path;
	if (!fs.FileExists(abs_path)) {
		throw IOException("capped_add_segment: file '%s' does not exist", abs_path);
	}

	int64_t rows = 0, bytes = 0, min_ts = 0, max_ts = 0;
	bool has_ts = false;
	int64_t seg_id = 0;
	{
		auto lock = capped_catalog.LockWrites();
		auto manifest = storage.ReadManifest(fs, table_dir);

		CappedString err, time_col;
		if (capped_manifest_time_col(manifest.c_str(), time_col.Receive(), err.Receive()) != CAPPED_OK) {
			throw IOException("capped_add_segment: reading time column failed: %s", err.Str());
		}
		Connection con(DatabaseInstance::GetDatabase(context));

		// DuckDB's parquet reader needs one uniform schema across a table's
		// segments, so an adopted file must carry exactly the table's columns
		// (the store's hidden ingest column is allowed but optional)
		vector<string> expected;
		int64_t ncols = 0;
		if (capped_manifest_column_count(manifest.c_str(), &ncols, err.Receive()) != CAPPED_OK) {
			throw IOException("capped_add_segment: reading table schema failed: %s", err.Str());
		}
		for (int64_t i = 0; i < ncols; i++) {
			CappedString col_name, col_type;
			if (capped_manifest_column(manifest.c_str(), i, col_name.Receive(), col_type.Receive(), err.Receive()) !=
			    CAPPED_OK) {
				throw IOException("capped_add_segment: reading table column %lld failed: %s", i, err.Str());
			}
			if (col_name.Str() != CAPPED_INGEST_TS_COLUMN) {
				expected.push_back(col_name.Str());
			}
		}
		// the hidden ingest column may or may not be there (rows without it have
		// no known arrival time); anything else is a schema drift
		auto file_cols = CappedWrite::ParquetColumns(con, abs_path);
		case_insensitive_set_t have(file_cols.begin(), file_cols.end());
		have.erase(CAPPED_INGEST_TS_COLUMN);
		if (have != case_insensitive_set_t(expected.begin(), expected.end())) {
			throw IOException("capped_add_segment: '%s' holds columns [%s] but table '%s' expects [%s] ('%s' optional)",
			                  bdata.rel_path, StringUtil::Join(file_cols, ", "), table_dir,
			                  StringUtil::Join(expected, ", "), CAPPED_INGEST_TS_COLUMN);
		}

		CappedWrite::ComputeSegmentStats(con, abs_path, time_col.Str(), rows, bytes, has_ts, min_ts, max_ts);

		if (capped_manifest_next_segment_id(manifest.c_str(), &seg_id, err.Receive()) != CAPPED_OK) {
			throw IOException("capped_add_segment: allocating segment id failed: %s", err.Str());
		}
		// unknown bounds stay null (a 0 bound would read as 1970 and age out)
		string bound = has_ts ? to_string(min_ts) : string("null");
		string bound_max = has_ts ? to_string(max_ts) : string("null");
		string seg_json = "{\"id\":" + to_string(seg_id) + ",\"path\":\"" + CappedEscapeJson(bdata.rel_path) +
		                  "\",\"rows\":" + to_string(rows) + ",\"bytes\":" + to_string(bytes) + ",\"min_ts\":" +
		                  bound + ",\"max_ts\":" + bound_max + ",\"sealed\":true,\"sealed_at\":" +
		                  to_string(NowMicros()) + "}";
		CappedString new_manifest;
		if (capped_manifest_put_segment(manifest.c_str(), seg_json.c_str(), new_manifest.Receive(), err.Receive()) !=
		    CAPPED_OK) {
			throw IOException("capped_add_segment: registering segment failed: %s", err.Str());
		}
		storage.WriteManifest(fs, table_dir, new_manifest.Str());
	}
	// caps apply to bulk-ingested data too
	CappedWrite::Enforce(capped_catalog, table_dir);

	output.SetCardinality(1);
	output.SetValue(0, 0, Value(StringUtil::Format("segment %lld: %lld rows, %lld bytes -> %s", seg_id, rows,
	                                               bytes, bdata.rel_path)));
}

void RegisterAddSegmentFunction(ExtensionLoader &loader) {
	TableFunction add_segment("capped_add_segment", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                          AddSegmentFunc, AddSegmentBind, AddSegmentInitGlobal);
	loader.RegisterFunction(add_segment);
}

// --- capped_status: per-segment store inspection ----------------------------

struct CappedStatusBindData : public TableFunctionData {
	string catalog_name;
	string table;
};

struct CappedStatusGlobalState : public GlobalTableFunctionState {
	struct Row {
		CappedSegmentInfo seg;
		//! has an index recorded *and* the file is actually there
		bool indexed = false;
		int64_t index_bytes = 0;
	};
	vector<Row> rows;
	CappedPolicySummary policy;
	idx_t next = 0;

	bool Finished() const {
		return next >= rows.size();
	}
};

unique_ptr<FunctionData> StatusBind(ClientContext &context, TableFunctionBindInput &input,
                                    vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 2) {
		throw BinderException("capped_status(catalog, table) takes two VARCHAR arguments");
	}
	auto bind_data = make_uniq<CappedStatusBindData>();
	bind_data->catalog_name = input.inputs[0].DefaultCastAs(LogicalType::VARCHAR).ToString();
	bind_data->table = input.inputs[1].DefaultCastAs(LogicalType::VARCHAR).ToString();

	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("segment_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("path");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("sealed");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("rows");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("bytes");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("min_ts_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("max_ts_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("time_col");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("ttl_us");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("max_bytes");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("max_rows");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("evict_order");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("on_violation");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("indexed");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("index_bytes");
	return_types.emplace_back(LogicalType::BIGINT);
	// append-only below (fragments plan §3.9): open-segment fragment file
	// count; 0 for sealed segments (the seal clears them)
	names.emplace_back("fragments");
	return_types.emplace_back(LogicalType::BIGINT);
	return bind_data;
}

unique_ptr<GlobalTableFunctionState> StatusInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bdata = input.bind_data->Cast<CappedStatusBindData>();
	auto result = make_uniq<CappedStatusGlobalState>();
	auto &capped_catalog = ResolveCappedCatalog(context, bdata.catalog_name, "capped_status");
	auto table_dir = ResolveCappedTable(capped_catalog, bdata.table, "capped_status");
	auto manifest = capped_catalog.Storage().ReadManifest(capped_catalog.FS(), table_dir);
	for (auto &seg : CappedReadSegments(manifest)) {
		CappedStatusGlobalState::Row row;
		row.seg = seg;
		if (seg.sealed && seg.has_index) {
			row.index_bytes = (int64_t)CappedIndex::BytesOnDisk(capped_catalog, seg.index_file);
			row.indexed = row.index_bytes > 0;
		}
		result->rows.push_back(std::move(row));
	}
	result->policy = CappedReadPolicySummary(manifest);
	return std::move(result);
}

void StatusFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<CappedStatusGlobalState>();
	auto &bdata = data.bind_data->Cast<CappedStatusBindData>();
	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE && !gstate.Finished()) {
		auto &row = gstate.rows[gstate.next++];
		auto &seg = row.seg;
		auto r = count++;
		output.SetValue(0, r, Value(bdata.table));
		output.SetValue(1, r, Value::BIGINT(seg.id));
		output.SetValue(2, r, Value(seg.path));
		output.SetValue(3, r, Value::BOOLEAN(seg.sealed));
		output.SetValue(4, r, Value::BIGINT(seg.rows));
		output.SetValue(5, r, Value::BIGINT(seg.bytes));
		output.SetValue(6, r, seg.has_ts ? Value::BIGINT(seg.min_ts) : Value(LogicalType::BIGINT));
		output.SetValue(7, r, seg.has_ts ? Value::BIGINT(seg.max_ts) : Value(LogicalType::BIGINT));
		output.SetValue(8, r, Value(gstate.policy.time_col));
		output.SetValue(9, r, gstate.policy.has_ttl ? Value::BIGINT(gstate.policy.ttl) : Value(LogicalType::BIGINT));
		output.SetValue(10, r, gstate.policy.has_max_bytes ? Value::BIGINT(gstate.policy.max_bytes)
		                                                   : Value(LogicalType::BIGINT));
		output.SetValue(11, r,
		                gstate.policy.has_max_rows ? Value::BIGINT(gstate.policy.max_rows) : Value(LogicalType::BIGINT));
		output.SetValue(12, r, Value(gstate.policy.evict_order));
		output.SetValue(13, r, Value(gstate.policy.on_violation));
		output.SetValue(14, r, Value::BOOLEAN(row.indexed));
		output.SetValue(15, r, Value::BIGINT(row.index_bytes));
		output.SetValue(16, r, Value::BIGINT(seg.fragments));
	}
	output.SetCardinality(count);
}

void RegisterStatusFunction(ExtensionLoader &loader) {
	TableFunction status("capped_status", {LogicalType::VARCHAR, LogicalType::VARCHAR}, StatusFunc, StatusBind,
	                     StatusInitGlobal);
	loader.RegisterFunction(status);
}

// --- capped_seal: rotate the open segment ------------------------------------

struct CappedSealBindData : public TableFunctionData {
	string catalog_name;
	string table;
};

struct CappedSealGlobalState : public GlobalTableFunctionState {
	idx_t emitted = 0;
	bool Finished() const {
		return emitted > 0;
	}
};

unique_ptr<FunctionData> SealBind(ClientContext &context, TableFunctionBindInput &input,
                                  vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 2) {
		throw BinderException("capped_seal(catalog, table) takes two VARCHAR arguments");
	}
	auto bind_data = make_uniq<CappedSealBindData>();
	bind_data->catalog_name = input.inputs[0].DefaultCastAs(LogicalType::VARCHAR).ToString();
	bind_data->table = input.inputs[1].DefaultCastAs(LogicalType::VARCHAR).ToString();
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("capped_seal");
	return bind_data;
}

unique_ptr<GlobalTableFunctionState> SealInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<CappedSealGlobalState>();
}

void SealFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<CappedSealGlobalState>();
	if (gstate.Finished()) {
		output.SetCardinality(0);
		return;
	}
	gstate.emitted++;
	auto &bdata = data.bind_data->Cast<CappedSealBindData>();
	auto &capped_catalog = ResolveCappedCatalog(context, bdata.catalog_name, "capped_seal");
	auto table_dir = ResolveCappedTable(capped_catalog, bdata.table, "capped_seal");

	auto sealed = CappedWrite::ForceSeal(capped_catalog, table_dir);
	output.SetCardinality(1);
	output.SetValue(0, 0,
	                Value(sealed ? StringUtil::Format("table '%s': open segment sealed", bdata.table)
	                             : StringUtil::Format("table '%s': no open segment", bdata.table)));
}

void RegisterSealFunction(ExtensionLoader &loader) {
	TableFunction seal("capped_seal", {LogicalType::VARCHAR, LogicalType::VARCHAR}, SealFunc, SealBind,
	                   SealInitGlobal);
	loader.RegisterFunction(seal);
}

// --- capped_enforce: apply retention on demand (idle stores, bulk loads) ----

struct CappedEnforceBindData : public TableFunctionData {
	string catalog_name;
	string table; // empty => all tables
};

struct CappedEnforceGlobalState : public GlobalTableFunctionState {
	struct Row {
		string table;
		int64_t dropped;
		bool sealed;
	};
	vector<Row> rows;
	idx_t next = 0;

	bool Finished() const {
		return next >= rows.size();
	}
};

unique_ptr<FunctionData> EnforceBind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() < 1 || input.inputs.size() > 2) {
		throw BinderException("capped_enforce(catalog [, table]) takes one or two VARCHAR arguments");
	}
	auto bind_data = make_uniq<CappedEnforceBindData>();
	bind_data->catalog_name = input.inputs[0].DefaultCastAs(LogicalType::VARCHAR).ToString();
	if (input.inputs.size() == 2) {
		bind_data->table = input.inputs[1].DefaultCastAs(LogicalType::VARCHAR).ToString();
	}
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("table_name");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("segments_dropped");
	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("open_sealed");
	return bind_data;
}

unique_ptr<GlobalTableFunctionState> EnforceInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bdata = input.bind_data->Cast<CappedEnforceBindData>();
	auto result = make_uniq<CappedEnforceGlobalState>();
	auto &capped_catalog = ResolveCappedCatalog(context, bdata.catalog_name, "capped_enforce");
	vector<string> tables;
	if (bdata.table.empty()) {
		tables = capped_catalog.Storage().ListTables(capped_catalog.FS());
	} else {
		tables.push_back(ResolveCappedTable(capped_catalog, bdata.table, "capped_enforce"));
	}
	for (auto &table : tables) {
		auto summary = CappedWrite::Enforce(capped_catalog, table);
		result->rows.push_back({table, (int64_t)summary.dropped, summary.sealed});
	}
	return std::move(result);
}

void EnforceFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<CappedEnforceGlobalState>();
	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE && !gstate.Finished()) {
		auto &row = gstate.rows[gstate.next++];
		auto r = count++;
		output.SetValue(0, r, Value(row.table));
		output.SetValue(1, r, Value::BIGINT(row.dropped));
		output.SetValue(2, r, Value::BOOLEAN(row.sealed));
	}
	output.SetCardinality(count);
}

void RegisterEnforceFunction(ExtensionLoader &loader) {
	TableFunction enforce_one("capped_enforce", {LogicalType::VARCHAR, LogicalType::VARCHAR}, EnforceFunc, EnforceBind,
	                          EnforceInitGlobal);
	TableFunction enforce_all("capped_enforce", {LogicalType::VARCHAR}, EnforceFunc, EnforceBind, EnforceInitGlobal);
	TableFunctionSet set("capped_enforce");
	set.AddFunction(enforce_one);
	set.AddFunction(enforce_all);
	loader.RegisterFunction(set);
}

// --- capped_tables: store overview -------------------------------------------

struct CappedTablesBindData : public TableFunctionData {
	string catalog_name;
};

struct CappedTablesGlobalState : public GlobalTableFunctionState {
	struct Row {
		string table;
		int64_t segments = 0;
		int64_t sealed = 0;
		bool has_open = false;
		int64_t rows = 0;
		int64_t bytes = 0;
		//! sealed segments with a live search index, and what those indexes cost
		//! on disk (overhead the caps deliberately do not count)
		int64_t indexed = 0;
		int64_t index_bytes = 0;
		bool has_range = false;
		int64_t min_ts = 0;
		int64_t max_ts = 0;
		//! fragment files of the open segment (0 when sealed or none)
		int64_t fragments = 0;
		//! post-commit maintenance failures (see CappedCatalog::RecordMaintenanceFailure)
		idx_t maintenance_failures = 0;
		string last_maintenance_error;
		bool has_maintenance_error = false;
		//! cumulative Parquet bytes capped wrote, and files queued in
		//! pending_deletes (fragments plan §3.9)
		int64_t bytes_written = 0;
		int64_t pending_delete_files = 0;
		CappedPolicySummary policy;
	};
	vector<Row> rows;
	idx_t next = 0;

	bool Finished() const {
		return next >= rows.size();
	}
};

unique_ptr<FunctionData> TablesBind(ClientContext &context, TableFunctionBindInput &input,
                                    vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1) {
		throw BinderException("capped_tables(catalog) takes one VARCHAR argument");
	}
	auto bind_data = make_uniq<CappedTablesBindData>();
	bind_data->catalog_name = input.inputs[0].DefaultCastAs(LogicalType::VARCHAR).ToString();
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("table_name");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("segments");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("sealed");
	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("open");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("rows");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("bytes");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("min_ts_us");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("max_ts_us");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("time_col");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("ttl_us");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("max_bytes");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("max_rows");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("evict_order");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("on_violation");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("indexed");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("index_bytes");
	// append-only below: lotel reads this table with SELECT * (fragments plan §3.9)
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("fragments");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("maintenance_failures");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("last_maintenance_error");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("bytes_written");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("pending_delete_files");
	return bind_data;
}

unique_ptr<GlobalTableFunctionState> TablesInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bdata = input.bind_data->Cast<CappedTablesBindData>();
	auto result = make_uniq<CappedTablesGlobalState>();
	auto &capped_catalog = ResolveCappedCatalog(context, bdata.catalog_name, "capped_tables");
	for (auto &table : capped_catalog.Storage().ListTables(capped_catalog.FS())) {
		auto manifest = capped_catalog.Storage().ReadManifest(capped_catalog.FS(), table);
		CappedTablesGlobalState::Row row;
		row.table = table;
		auto stats = CappedReadManifestStats(manifest);
		row.bytes_written = stats.bytes_written;
		row.pending_delete_files = stats.pending_delete_files;
		for (auto &seg : CappedReadSegments(manifest)) {
			row.segments++;
			row.rows += seg.rows;
			row.bytes += seg.bytes;
			if (seg.sealed) {
				row.sealed++;
				if (seg.has_index) {
					auto index_bytes = (int64_t)CappedIndex::BytesOnDisk(capped_catalog, seg.index_file);
					if (index_bytes > 0) {
						row.indexed++;
						row.index_bytes += index_bytes;
					}
				}
			} else {
				row.has_open = true;
				row.fragments += seg.fragments;
			}
			if (!seg.has_ts) {
				continue; // segments without an event time contribute no range
			}
			if (!row.has_range) {
				row.min_ts = seg.min_ts;
				row.max_ts = seg.max_ts;
				row.has_range = true;
			} else {
				row.min_ts = MinValue(row.min_ts, seg.min_ts);
				row.max_ts = MaxValue(row.max_ts, seg.max_ts);
			}
		}
		row.policy = CappedReadPolicySummary(manifest);
		row.has_maintenance_error =
		    capped_catalog.GetMaintenanceFailure(table, row.maintenance_failures, row.last_maintenance_error);
		result->rows.push_back(std::move(row));
	}
	return std::move(result);
}

void TablesFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<CappedTablesGlobalState>();
	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE && !gstate.Finished()) {
		auto &row = gstate.rows[gstate.next++];
		auto r = count++;
		output.SetValue(0, r, Value(row.table));
		output.SetValue(1, r, Value::BIGINT(row.segments));
		output.SetValue(2, r, Value::BIGINT(row.sealed));
		output.SetValue(3, r, Value::BOOLEAN(row.has_open));
		output.SetValue(4, r, Value::BIGINT(row.rows));
		output.SetValue(5, r, Value::BIGINT(row.bytes));
		output.SetValue(6, r, row.has_range ? Value::BIGINT(row.min_ts) : Value(LogicalType::BIGINT));
		output.SetValue(7, r, row.has_range ? Value::BIGINT(row.max_ts) : Value(LogicalType::BIGINT));
		output.SetValue(8, r, Value(row.policy.time_col));
		output.SetValue(9, r,
		                row.policy.has_ttl ? Value::BIGINT(row.policy.ttl) : Value(LogicalType::BIGINT));
		output.SetValue(10, r, row.policy.has_max_bytes ? Value::BIGINT(row.policy.max_bytes)
		                                                : Value(LogicalType::BIGINT));
		output.SetValue(11, r,
		                row.policy.has_max_rows ? Value::BIGINT(row.policy.max_rows) : Value(LogicalType::BIGINT));
		output.SetValue(12, r, Value(row.policy.evict_order));
		output.SetValue(13, r, Value(row.policy.on_violation));
		output.SetValue(14, r, Value::BIGINT(row.indexed));
		output.SetValue(15, r, Value::BIGINT(row.index_bytes));
		output.SetValue(16, r, Value::BIGINT(row.fragments));
		output.SetValue(17, r, Value::BIGINT(NumericCast<int64_t>(row.maintenance_failures)));
		output.SetValue(18, r, row.has_maintenance_error ? Value(row.last_maintenance_error)
		                                                 : Value(LogicalType::VARCHAR));
		output.SetValue(19, r, Value::BIGINT(row.bytes_written));
		output.SetValue(20, r, Value::BIGINT(row.pending_delete_files));
	}
	output.SetCardinality(count);
}

void RegisterTablesFunction(ExtensionLoader &loader) {
	TableFunction tables("capped_tables", {LogicalType::VARCHAR}, TablesFunc, TablesBind, TablesInitGlobal);
	loader.RegisterFunction(tables);
}

// --- capped_index: build + inspect per-segment search indexes ----------------

struct CappedIndexBuildBindData : public TableFunctionData {
	string catalog_name;
	string table;
	bool rebuild = false;
};

struct CappedIndexBuildGlobalState : public GlobalTableFunctionState {
	struct Row {
		int64_t segment_id = 0;
		//! false for a note row: a table with no segments still has to say why
		//! its search is empty instead of returning zero rows
		bool has_segment = false;
		string path;
		bool sealed = false;
		bool indexed = false;
		int64_t index_bytes = 0;
	};
	vector<Row> rows;
	string build_error;
	idx_t next = 0;

	bool Finished() const {
		return next >= rows.size();
	}
};

unique_ptr<FunctionData> IndexBuildBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 2) {
		throw BinderException("capped_index(catalog, table) takes two VARCHAR arguments");
	}
	auto bind_data = make_uniq<CappedIndexBuildBindData>();
	bind_data->catalog_name = input.inputs[0].DefaultCastAs(LogicalType::VARCHAR).ToString();
	bind_data->table = input.inputs[1].DefaultCastAs(LogicalType::VARCHAR).ToString();
	auto rebuild = input.named_parameters.find("rebuild");
	if (rebuild != input.named_parameters.end()) {
		bind_data->rebuild = rebuild->second.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
	}
	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("segment_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("path");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("sealed");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("indexed");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("index_bytes");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("build_error");
	return_types.emplace_back(LogicalType::VARCHAR);
	return bind_data;
}

unique_ptr<GlobalTableFunctionState> IndexBuildInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bdata = input.bind_data->Cast<CappedIndexBuildBindData>();
	auto result = make_uniq<CappedIndexBuildGlobalState>();
	auto &capped_catalog = ResolveCappedCatalog(context, bdata.catalog_name, "capped_index");
	auto table_dir = ResolveCappedTable(capped_catalog, bdata.table, "capped_index");

	// explicit maintenance always tries, even after an earlier failure
	string error;
	CappedIndex::Ensure(capped_catalog, table_dir, error,
	                    bdata.rebuild ? CappedIndexMode::Rebuild : CappedIndexMode::Retry);
	result->build_error = error;

	auto manifest = capped_catalog.Storage().ReadManifest(capped_catalog.FS(), table_dir);
	for (auto &seg : CappedReadSegments(manifest)) {
		CappedIndexBuildGlobalState::Row row;
		row.segment_id = seg.id;
		row.has_segment = true;
		row.path = seg.path;
		row.sealed = seg.sealed;
		if (seg.sealed && seg.has_index) {
			row.index_bytes = (int64_t)CappedIndex::BytesOnDisk(capped_catalog, seg.index_file);
			row.indexed = row.index_bytes > 0;
		}
		result->rows.push_back(std::move(row));
	}
	if (result->rows.empty()) {
		result->rows.push_back(CappedIndexBuildGlobalState::Row());
	}
	return std::move(result);
}

void IndexBuildFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<CappedIndexBuildGlobalState>();
	auto &bdata = data.bind_data->Cast<CappedIndexBuildBindData>();
	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE && !gstate.Finished()) {
		auto &row = gstate.rows[gstate.next++];
		auto r = count++;
		output.SetValue(0, r, Value(bdata.table));
		output.SetValue(1, r, row.has_segment ? Value::BIGINT(row.segment_id) : Value(LogicalType::BIGINT));
		output.SetValue(2, r, row.has_segment ? Value(row.path) : Value(LogicalType::VARCHAR));
		output.SetValue(3, r, row.has_segment ? Value::BOOLEAN(row.sealed) : Value(LogicalType::BOOLEAN));
		output.SetValue(4, r, Value::BOOLEAN(row.indexed));
		output.SetValue(5, r, Value::BIGINT(row.index_bytes));
		output.SetValue(6, r, gstate.build_error.empty() ? Value(LogicalType::VARCHAR) : Value(gstate.build_error));
	}
	output.SetCardinality(count);
}

void RegisterIndexBuildFunction(ExtensionLoader &loader) {
	TableFunction index_fn("capped_index", {LogicalType::VARCHAR, LogicalType::VARCHAR}, IndexBuildFunc,
	                       IndexBuildBind, IndexBuildInitGlobal);
	index_fn.named_parameters["rebuild"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(index_fn);
}

// --- capped_fts / capped_vector_search: search over sealed segments ---------
//
// Both fan out over the table's *live* indexed segments and merge the per-segment
// top-k here, so results follow retention automatically: an evicted segment stops
// being searchable at the same instant it stops being scannable.

struct CappedSearchBindData : public TableFunctionData {
	string catalog_name;
	string table;
	string query;
	//! only used by capped_vector_search
	Value query_vector;
	int64_t k = 10;
};

struct CappedSearchGlobalState : public GlobalTableFunctionState {
	CappedSearchResult result;
	idx_t next = 0;

	bool Finished() const {
		return next >= result.hits.size();
	}
};

int64_t ReadSearchK(const TableFunctionBindInput &input, idx_t positional_index) {
	auto named = input.named_parameters.find("k");
	if (named != input.named_parameters.end()) {
		return named->second.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
	}
	if (input.inputs.size() > positional_index) {
		return input.inputs[positional_index].DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
	}
	return 10;
}

//! segment_id, row_id, score, then the searchable text columns of the table
void DeclareSearchColumns(vector<LogicalType> &return_types, vector<string> &names, const vector<string> &payload) {
	names.emplace_back("segment_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("row_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("score");
	return_types.emplace_back(LogicalType::DOUBLE);
	for (auto &column : payload) {
		names.push_back(column);
		return_types.emplace_back(LogicalType::VARCHAR);
	}
}

void EmitSearchHits(TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<CappedSearchGlobalState>();
	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE && !gstate.Finished()) {
		auto &hit = gstate.result.hits[gstate.next++];
		auto r = count++;
		output.SetValue(0, r, Value::BIGINT(hit.segment_id));
		output.SetValue(1, r, Value::BIGINT(hit.row_id));
		output.SetValue(2, r, Value::DOUBLE(hit.score));
		for (idx_t c = 0; c < hit.values.size(); c++) {
			output.SetValue(3 + c, r, hit.values[c]);
		}
	}
	output.SetCardinality(count);
}

unique_ptr<FunctionData> FtsBind(ClientContext &context, TableFunctionBindInput &input,
                                 vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() < 3 || input.inputs.size() > 4) {
		throw BinderException("capped_fts(catalog, table, query [, k]) takes three or four arguments");
	}
	auto bind_data = make_uniq<CappedSearchBindData>();
	bind_data->catalog_name = input.inputs[0].DefaultCastAs(LogicalType::VARCHAR).ToString();
	bind_data->table = input.inputs[1].DefaultCastAs(LogicalType::VARCHAR).ToString();
	bind_data->query = input.inputs[2].DefaultCastAs(LogicalType::VARCHAR).ToString();
	bind_data->k = ReadSearchK(input, 3);

	// resolve the searchable columns now, so a table without them fails to bind
	// rather than returning an empty result at execution time
	auto &capped_catalog = ResolveCappedCatalog(context, bind_data->catalog_name, "capped_fts");
	auto table_dir = ResolveCappedTable(capped_catalog, bind_data->table, "capped_fts");
	auto manifest = capped_catalog.Storage().ReadManifest(capped_catalog.FS(), table_dir);
	auto spec = CappedReadSearchSpec(manifest);
	if (spec.columns.empty()) {
		throw BinderException("capped_fts: table '%s' has no full-text search columns; create it WITH "
		                      "(search_columns='...')",
		                      bind_data->table);
	}
	DeclareSearchColumns(return_types, names, spec.columns);
	return bind_data;
}

unique_ptr<GlobalTableFunctionState> FtsInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bdata = input.bind_data->Cast<CappedSearchBindData>();
	auto result = make_uniq<CappedSearchGlobalState>();
	auto &capped_catalog = ResolveCappedCatalog(context, bdata.catalog_name, "capped_fts");
	auto table_dir = ResolveCappedTable(capped_catalog, bdata.table, "capped_fts");
	result->result = CappedIndex::SearchText(capped_catalog, table_dir, bdata.query, bdata.k);
	return std::move(result);
}

void FtsFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	EmitSearchHits(data, output);
}

unique_ptr<FunctionData> VectorSearchBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() < 3 || input.inputs.size() > 4) {
		throw BinderException("capped_vector_search(catalog, table, vector [, k]) takes three or four arguments");
	}
	auto bind_data = make_uniq<CappedSearchBindData>();
	bind_data->catalog_name = input.inputs[0].DefaultCastAs(LogicalType::VARCHAR).ToString();
	bind_data->table = input.inputs[1].DefaultCastAs(LogicalType::VARCHAR).ToString();
	bind_data->query_vector = input.inputs[2];
	bind_data->k = ReadSearchK(input, 3);
	string column;
	// "column" would be the natural name but it is a reserved word, so the named
	// parameter could never be written; the policy option is vector_columns
	auto named = input.named_parameters.find("vector_column");
	if (named != input.named_parameters.end()) {
		column = named->second.DefaultCastAs(LogicalType::VARCHAR).ToString();
	}

	auto &capped_catalog = ResolveCappedCatalog(context, bind_data->catalog_name, "capped_vector_search");
	auto table_dir = ResolveCappedTable(capped_catalog, bind_data->table, "capped_vector_search");
	auto manifest = capped_catalog.Storage().ReadManifest(capped_catalog.FS(), table_dir);
	auto spec = CappedReadSearchSpec(manifest);
	if (spec.vector_columns.empty()) {
		throw BinderException("capped_vector_search: table '%s' has no vector columns; create it WITH "
		                      "(vector_columns='...')",
		                      bind_data->table);
	}
	if (!column.empty() && spec.vector_columns.size() == 1 &&
	    !StringUtil::CIEquals(column, spec.vector_columns[0])) {
		throw BinderException("capped_vector_search: table '%s' indexes '%s', not '%s'", bind_data->table,
		                      spec.vector_columns[0], column);
	}
	DeclareSearchColumns(return_types, names, spec.columns);
	// the column choice is validated for real inside SearchVector, which also
	// needs the metric and dimension from the same manifest read
	bind_data->query = column;
	return bind_data;
}

unique_ptr<GlobalTableFunctionState> VectorSearchInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bdata = input.bind_data->Cast<CappedSearchBindData>();
	auto result = make_uniq<CappedSearchGlobalState>();
	auto &capped_catalog = ResolveCappedCatalog(context, bdata.catalog_name, "capped_vector_search");
	auto table_dir = ResolveCappedTable(capped_catalog, bdata.table, "capped_vector_search");
	result->result = CappedIndex::SearchVector(capped_catalog, table_dir, bdata.query, bdata.query_vector, bdata.k);
	return std::move(result);
}

void VectorSearchFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	EmitSearchHits(data, output);
}

void RegisterSearchFunctions(ExtensionLoader &loader) {
	auto add_named = [](TableFunction &fn) {
		fn.named_parameters["k"] = LogicalType::BIGINT;
	};
	TableFunction fts("capped_fts", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, FtsFunc,
	                  FtsBind, FtsInitGlobal);
	add_named(fts);
	TableFunction fts_k("capped_fts",
	                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT},
	                   FtsFunc, FtsBind, FtsInitGlobal);
	add_named(fts_k);
	TableFunctionSet fts_set("capped_fts");
	fts_set.AddFunction(fts);
	fts_set.AddFunction(fts_k);
	loader.RegisterFunction(fts_set);

	// the vector argument is ANY on purpose: [1,2,3] binds as LIST, [1,2,3]::FLOAT[3]
	// as ARRAY, and both are legitimate ways to write a query vector
	TableFunction vec("capped_vector_search",
	                  {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::ANY}, VectorSearchFunc,
	                  VectorSearchBind, VectorSearchInitGlobal);
	vec.named_parameters["k"] = LogicalType::BIGINT;
	vec.named_parameters["vector_column"] = LogicalType::VARCHAR;
	TableFunction vec_k("capped_vector_search",
	                    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::ANY, LogicalType::BIGINT},
	                    VectorSearchFunc, VectorSearchBind, VectorSearchInitGlobal);
	vec_k.named_parameters["k"] = LogicalType::BIGINT;
	vec_k.named_parameters["vector_column"] = LogicalType::VARCHAR;
	TableFunctionSet vec_set("capped_vector_search");
	vec_set.AddFunction(vec);
	vec_set.AddFunction(vec_k);
	loader.RegisterFunction(vec_set);
}

//! Reject unparseable grace values where the user is looking: PhysicalSet
//! calls this after casting and before storing, so a typo fails the SET and
//! the bad value never reaches the store-wide reader.
void CheckPendingDeleteGrace(ClientContext &, SetScope, Value &parameter) {
	int64_t unused = 0;
	if (parameter.IsNull() || parameter.type() != LogicalType::VARCHAR ||
	    !CappedWrite::TryParseGraceMicros(parameter.GetValue<string>(), unused)) {
		throw InvalidInputException("SET capped_pending_delete_grace: '%s' is not a duration "
		                            "(examples: '5 min', '30s', '250ms', '0' to collect immediately)",
		                            parameter.IsNull() ? "<NULL>" : parameter.ToString());
	}
}

void LoadInternal(ExtensionLoader &loader) {
	RegisterStorageExtension(loader);
	// Test-only knob (T7): the orphan-GC age floor and the grace newly
	// queued pending deletes get. Default = the Rust DELETE_GRACE (5 min);
	// "0" collects crash leftovers the moment an ATTACH sees them. Stored
	// not_before values in a manifest are never rewritten.
	auto &db = loader.GetDatabaseInstance();
	DBConfig::GetConfig(db).AddExtensionOption(
	    "capped_pending_delete_grace",
	    "capped: grace for orphan-GC candidates and newly queued pending deletes (duration, default 5 min)",
	    LogicalType::VARCHAR, Value("5 min"), CheckPendingDeleteGrace, SetScope::GLOBAL);
	RegisterVersionFunction(loader);
	RegisterAddSegmentFunction(loader);
	RegisterStatusFunction(loader);
	RegisterSealFunction(loader);
	RegisterEnforceFunction(loader);
	RegisterTablesFunction(loader);
	RegisterIndexBuildFunction(loader);
	RegisterSearchFunctions(loader);
}

} // namespace

void CappedExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string CappedExtension::Name() {
	return "capped";
}

std::string CappedExtension::Version() const {
#ifdef EXT_VERSION_CAPPED
	return EXT_VERSION_CAPPED;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(capped, loader) {
	duckdb::LoadInternal(loader);
}
}
