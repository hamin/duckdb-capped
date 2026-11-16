#pragma once

#include "capped_storage_info.hpp"
#include "capped_table.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

#include <unordered_map>

namespace duckdb {

//! The single 'main' schema of a capped store. Table entries are built from
//! manifests on demand and cached for the lifetime of the ATTACH.
class CappedSchema : public SchemaCatalogEntry {
public:
	CappedSchema(Catalog &catalog, CreateSchemaInfo &info, CappedStorageInfo &storage);

	//! Enumerate tables of the given type (only TABLE_ENTRY is real).
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;

	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup) override;

	//! CREATE TABLE ... [WITH (time_col=..., ttl=..., max_bytes=..., max_rows=...,
	//! grace=..., evict_order=..., on_violation=...)] — persists a manifest.
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;

	//! DROP TABLE — removes the table directory (manifest + segments).
	void DropEntry(ClientContext &context, DropInfo &info) override;

	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
	optional_ptr<CatalogEntry> CreateCoordinateSystem(CatalogTransaction transaction,
	                                                  CreateCoordinateSystemInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;

private:
	//! Load (or return cached) table entry from its manifest; nullptr if the
	//! table has no manifest.
	optional_ptr<CatalogEntry> LoadEntry(const string &table);
	//! Parse WITH (key=value) constant options into a policy spec JSON.
	static string PolicySpecFromOptions(CreateTableInfo &base);
	//! Filesystem of the attached database backing this schema.
	FileSystem &FS();

private:
	CappedStorageInfo &storage;
	std::unordered_map<string, unique_ptr<CappedTableEntry>> entries;
};

} // namespace duckdb
