#pragma once

#include "capped_storage_info.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

//! A capped table exposed from its manifest. Schema comes from the manifest's
//! columns; scan/insert paths arrive in the next milestones (M1b/M1c) and
//! currently fail with a clear message rather than pretending to work.
class CappedTableEntry : public TableCatalogEntry {
public:
	CappedTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, unique_ptr<CreateTableInfo> create_info);

	//! Build the CreateTableInfo (name + typed columns) from a manifest JSON.
	static unique_ptr<CreateTableInfo> BuildCreateTableInfo(Catalog &catalog, const string &table,
	                                                        const string &manifest_json);

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override {
		return nullptr; // no statistics yet; fine for planning
	}

	//! Capped tables have no row identity (append-only, whole-file eviction):
	//! no rowid virtual column.
	virtual_column_map_t GetVirtualColumns() const override {
		return {};
	}

	//! 2.0 made GetColumns() pure virtual; the manifest-built column list is
	//! the one in the CreateTableInfo we keep alive below.
	const ColumnList &GetColumns() const override {
		return create_info->columns;
	}

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                              const EntryLookupInfo &lookup_info) override;

	TableStorageInfo GetStorageInfo(ClientContext &context) override;

private:
	//! Keeps the CreateTableInfo that the base class references alive.
	unique_ptr<CreateTableInfo> create_info;
};

} // namespace duckdb
