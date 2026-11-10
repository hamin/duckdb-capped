#pragma once

#include "capped_schema.hpp"
#include "capped_storage_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/storage/database_size.hpp"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace duckdb {

//! Catalog over a capped store directory. Schemas and tables are not
//! persisted as catalog objects — the ground truth is <root>/<table>/
//! manifest.json, read fresh on every lookup (like other external catalogs).
class CappedCatalog : public Catalog {
public:
	CappedCatalog(AttachedDatabase &db, CappedStorageInfo storage);
	~CappedCatalog() override;

	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return "capped";
	}

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &lookup,
	                                              OnEntryNotFound if_not_found) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
	void DropSchema(ClientContext &context, DropInfo &info) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;

	//! Default rejects WITH options for non-Duck catalogs; capped tables are
	//! configured through them, so options stay and are validated by Rust.
	ErrorData SupportsCreateTable(BoundCreateTableInfo &info) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
	                                    LogicalCreateTable &op, PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	bool InMemory() override;
	string GetDBPath() override;

	CappedStorageInfo &Storage() {
		return storage;
	}
	CappedSchema &MainSchema() {
		return *main_schema;
	}
	//! The filesystem the store directory lives on (local, for now). Owned by
	//! the catalog: DuckDB's db-level fs wrappers are read-only opener views,
	//! but the store needs writes.
	FileSystem &FS() {
		return *local_fs;
	}

	//! Serialize manifest read-modify-write cycles (commit flush, add_segment,
	//! seal, eviction). Scans read manifests lock-free: writes land atomically
	//! via write-then-rename.
	std::unique_lock<std::mutex> LockWrites() {
		return std::unique_lock<std::mutex>(write_mutex);
	}

	//! Search index health. A failed build (usually fts/vss not being loadable)
	//! latches so the write path stops retrying on every commit; an ATTACH-time
	//! retry or an explicit capped_index() clears it and tries again.
	bool SearchDegraded() const {
		return search_degraded;
	}
	void SetSearchDegraded(bool degraded) {
		search_degraded = degraded;
	}

protected:
	CatalogEntryLookup TryLookupEntryInternal(CatalogTransaction transaction, const string &schema,
	                                          const EntryLookupInfo &lookup_info) override;

private:
	//! Body of the background enforcement thread (started in Initialize when
	//! storage.EnforceIntervalMicros() > 0; joined in the destructor).
	void EnforceLoop();

	CappedStorageInfo storage;
	unique_ptr<CappedSchema> main_schema;
	unique_ptr<LocalFileSystem> local_fs;
	std::mutex write_mutex;
	std::atomic<bool> search_degraded {false};

	std::thread enforce_thread;
	std::mutex stop_mutex;
	std::condition_variable stop_cv;
	std::atomic<bool> stop_requested {false};
};

} // namespace duckdb
