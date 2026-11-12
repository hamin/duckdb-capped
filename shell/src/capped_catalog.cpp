#include "capped_catalog.hpp"

#include "capped_table.hpp"
#include "capped_write.hpp"
#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

CappedCatalog::CappedCatalog(AttachedDatabase &db, CappedStorageInfo storage, bool read_only)
    : Catalog(db), storage(std::move(storage)), read_only(read_only) {
	local_fs = make_uniq<LocalFileSystem>();
	CreateSchemaInfo schema_info; // 2.0: CreateInfo defaults the schema slot to "main"
	main_schema = make_uniq<CappedSchema>(*this, schema_info, this->storage);
}

CappedCatalog::~CappedCatalog() {
	// stop + join the background enforcement thread before any member dies;
	// the thread only touches storage + local_fs, which outlive the join
	{
		std::lock_guard<std::mutex> guard(stop_mutex);
		stop_requested = true;
	}
	stop_cv.notify_all();
	if (enforce_thread.joinable()) {
		enforce_thread.join();
	}
}

void CappedCatalog::Initialize(bool load_builtin) {
	// Nothing to pre-load: manifests are read on demand. Start the background
	// enforcement thread unless disabled (enforce_interval => 0) or the store
	// is READ_ONLY (a READ_ONLY attach must write nothing, even when idle),
	// so an idle writable store still drops expired segments between writes.
	if (storage.EnforceIntervalMicros() > 0 && !read_only) {
		enforce_thread = std::thread([this] { EnforceLoop(); });
	}
}

void CappedCatalog::EnforceLoop() {
	while (true) {
		{
			std::unique_lock<std::mutex> guard(stop_mutex);
			stop_cv.wait_for(guard, std::chrono::microseconds(storage.EnforceIntervalMicros()),
			                 [this] { return stop_requested.load(); });
			if (stop_requested) {
				return;
			}
		}
		// Best-effort sweep: never let one bad table (or a store that is mid
		// DROP) take down the loop or surface to a client.
		vector<string> tables;
		try {
			tables = storage.ListTables(*local_fs);
		} catch (...) {
			continue;
		}
		for (auto &table : tables) {
			if (stop_requested) {
				return;
			}
			try {
				CappedWrite::Enforce(*this, table);
			} catch (...) {
			}
		}
	}
}

optional_ptr<CatalogEntry> CappedCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw NotImplementedException("capped: the store has a single implicit 'main' schema; CREATE SCHEMA is not "
	                              "supported");
}

optional_ptr<SchemaCatalogEntry> CappedCatalog::LookupSchema(CatalogTransaction transaction,
                                                             const EntryLookupInfo &lookup,
                                                             OnEntryNotFound if_not_found) {
	if (StringUtil::CIEquals(lookup.GetEntryName(), "main")) {
		return main_schema.get();
	}
	return nullptr;
}

void CappedCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	callback(*main_schema);
}

void CappedCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("capped: the store has a single implicit 'main' schema; DROP SCHEMA is not "
	                              "supported");
}

DatabaseSize CappedCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize size; // blocks are a DuckDB-internal concept; we report bytes
	auto &fs = FS();
	for (auto &table : storage.ListTables(fs)) {
		for (auto &entry : fs.Glob(storage.TablePath(table) + "/*", nullptr)) {
			if (!fs.FileExists(entry.path)) {
				continue;
			}
			auto handle = fs.OpenFile(entry.path, FileFlags::FILE_FLAGS_READ);
			auto bytes = fs.GetFileSize(*handle);
			if (bytes > 0) {
				size.bytes += static_cast<idx_t>(bytes);
			}
		}
	}
	return size;
}

ErrorData CappedCatalog::SupportsCreateTable(BoundCreateTableInfo &info) {
	auto &base = info.Base().Cast<CreateTableInfo>();
	if (!base.partition_keys.empty()) {
		return ErrorData(ExceptionType::CATALOG, "capped: PARTITIONED BY is not supported");
	}
	if (!base.sort_keys.empty()) {
		return ErrorData(ExceptionType::CATALOG, "capped: SORTED BY is not supported");
	}
	// WITH (time_col/ttl/max_bytes/...) is the point of a capped table; the
	// options themselves are validated by capped-core during CreateTable.
	return ErrorData();
}

// --- DML planning: arrives with the write path (milestone M1c) -------------

PhysicalOperator &CappedCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("capped: CREATE TABLE AS SELECT is not supported");
}

PhysicalOperator &CappedCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                             optional_ptr<PhysicalOperator> plan) {
	auto &entry = op.table.Cast<CappedTableEntry>();
	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		throw NotImplementedException("capped: ON CONFLICT is not supported (append-only tables have no keys)");
	}
	if (op.return_chunk) {
		throw NotImplementedException("capped: INSERT ... RETURNING is not supported yet");
	}
	if (!plan) {
		throw NotImplementedException("capped: INSERT without a source query is not supported");
	}
	// INSERT INTO t VALUES (...) arrives without explicit default projections;
	// make the child emit all table columns in schema order.
	if (!op.column_index_map.empty()) {
		plan = planner.ResolveDefaultsProjection(op, *plan);
	}
	auto &insert = planner.Make<CappedInsert>(entry, op.estimated_cardinality);
	insert.children.push_back(*plan);
	return insert;
}

PhysicalOperator &CappedCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                            PhysicalOperator &plan) {
	throw NotImplementedException("capped: DELETE is not supported — capped tables forget data via retention");
}

PhysicalOperator &CappedCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                            PhysicalOperator &plan) {
	throw NotImplementedException("capped: UPDATE is not supported — capped tables are append-only");
}

bool CappedCatalog::InMemory() {
	return false;
}

string CappedCatalog::GetDBPath() {
	return storage.Root();
}

CatalogEntryLookup CappedCatalog::TryLookupEntryInternal(CatalogTransaction transaction,
                                                         const EntryLookupInfo &lookup_info) {
	CatalogEntryLookup result;
	// 2.0 carries the schema in lookup_info (empty = unqualified -> resolve to main)
	auto &schema = lookup_info.GetSchema();
	if (!schema.empty() && !StringUtil::CIEquals(schema.GetIdentifierName(), "main")) {
		return result; // unknown schema: plain not-found
	}
	result.schema = main_schema.get();
	result.entry = main_schema->LookupEntry(transaction, lookup_info);
	return result;
}

void CappedCatalog::RecordMaintenanceFailure(const string &table, const string &error) {
	std::lock_guard<std::mutex> guard(maintenance_mutex);
	auto &stats = maintenance_failures[table];
	stats.failures++;
	stats.last_error = error;
}

bool CappedCatalog::GetMaintenanceFailure(const string &table, idx_t &failures, string &last_error) const {
	std::lock_guard<std::mutex> guard(maintenance_mutex);
	auto entry = maintenance_failures.find(table);
	if (entry == maintenance_failures.end()) {
		failures = 0;
		last_error.clear();
		return false;
	}
	failures = entry->second.failures;
	last_error = entry->second.last_error;
	return true;
}

} // namespace duckdb
