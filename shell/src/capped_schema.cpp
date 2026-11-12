#include "capped_schema.hpp"

#include "capped_catalog.hpp"
#include "capped_table.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"
#include "duckdb/parser/parsed_data/create_coordinate_system_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

CappedSchema::CappedSchema(Catalog &catalog, CreateSchemaInfo &info, CappedStorageInfo &storage)
    : SchemaCatalogEntry(catalog, info), storage(storage) {
}

FileSystem &CappedSchema::FS() {
	// same filesystem as the catalog: local, for now
	return catalog.Cast<CappedCatalog>().FS();
}

void CappedSchema::Scan(ClientContext &context, CatalogType type,
                        const std::function<void(CatalogEntry &)> &callback) {
	Scan(type, callback);
}

void CappedSchema::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		return; // the store only holds tables
	}
	for (auto &table : storage.ListTables(FS())) {
		auto entry = LoadEntry(table);
		if (entry) {
			callback(*entry);
		}
	}
}

optional_ptr<CatalogEntry> CappedSchema::LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup) {
	if (lookup.GetCatalogType() != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}
	return LoadEntry(lookup.GetEntryName());
}

optional_ptr<CatalogEntry> CappedSchema::LoadEntry(const string &table) {
	auto key = StringUtil::Lower(table);
	auto cached = entries.find(key);
	if (cached != entries.end()) {
		return optional_ptr<CatalogEntry>(cached->second.get());
	}
	// exact match first, then case-insensitive (DuckDB names are case-insensitive)
	auto &fs = FS();
	string dir = table;
	if (!storage.ManifestExists(fs, dir)) {
		dir.clear();
		for (auto &candidate : storage.ListTables(fs)) {
			if (StringUtil::CIEquals(candidate, table)) {
				dir = candidate;
				break;
			}
		}
		if (dir.empty()) {
			return nullptr;
		}
	}
	auto manifest = storage.ReadManifest(fs, dir);
	auto info = CappedTableEntry::BuildCreateTableInfo(catalog, dir, manifest);
	auto entry = make_uniq<CappedTableEntry>(catalog, *this, std::move(info));
	auto result = optional_ptr<CatalogEntry>(entry.get());
	entries[key] = std::move(entry);
	return result;
}

string CappedSchema::PolicySpecFromOptions(CreateTableInfo &base) {
	static const case_insensitive_set_t VALID_OPTIONS {"time_col",
	                                                   "ttl",
	                                                   "max_bytes",
	                                                   "max_rows",
	                                                   "grace",
	                                                   "evict_order",
	                                                   "on_violation",
	                                                   "search_columns",
	                                                   "vector_columns",
	                                                   "vector_metric"};
	string out = "{";
	bool first = true;
	for (auto &entry : base.options) {
		if (!VALID_OPTIONS.count(entry.first)) {
			throw ParserException("capped: unrecognized table option '%s'; expected one of time_col, ttl, "
			                      "max_bytes, max_rows, grace, evict_order, on_violation, search_columns, "
			                      "vector_columns, vector_metric",
			                      entry.first);
		}
		auto &expr = *entry.second;
		if (expr.GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
			throw ParserException("capped: option '%s' must be a constant value", entry.first);
		}
		auto &constant = expr.Cast<ConstantExpression>();
		// 2.0: ConstantExpression holds a Literal; go via Value for the same
		// string form the old constant.value.ToString() produced
		auto value = constant.GetLiteral().ToValue().ToString();
		// no time_col (or '' / '__ingest__') means "age by arrival": capped-core
		// turns that into TimeSource::IngestTime and the table gains the hidden
		// CAPPED_INGEST_TS_COLUMN, which the flush path stamps with now()
		if (!first) {
			out += ",";
		}
		first = false;
		out += "\"" + CappedEscapeJson(entry.first) + "\":\"" + CappedEscapeJson(value) + "\"";
	}
	out += "}";
	return out;
}

optional_ptr<CatalogEntry> CappedSchema::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	auto &base = info.Base();
	if (catalog.Cast<CappedCatalog>().ReadOnly()) {
		throw IOException("capped: cannot create table '%s': the store is attached READ_ONLY",
		                  base.GetTableName().GetIdentifierName());
	}
	if (base.query) {
		throw NotImplementedException("capped: CREATE TABLE AS SELECT into a capped table is not supported yet");
	}
	if (!base.constraints.empty()) {
		throw NotImplementedException("capped: constraints on capped tables are not supported yet");
	}
	if (base.columns.LogicalColumnCount() == 0) {
		throw ParserException("capped: a capped table needs at least one column");
	}

	// we have no CatalogSet dedupe: manifests are the catalog, so check here
	auto &fs = FS();
	// serialize with the write path + background enforcement (a concurrent
	// enforce of the same table must not interleave with this manifest write)
	auto lock = catalog.Cast<CappedCatalog>().LockWrites();
	if (storage.ManifestExists(fs, base.GetTableName().GetIdentifierName())) {
		if (base.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			return LoadEntry(base.GetTableName().GetIdentifierName());
		}
		if (base.on_conflict != OnCreateConflict::REPLACE_ON_CONFLICT) {
			throw CatalogException("Table with name \"%s\" already exists!",
			                       base.GetTableName().GetIdentifierName());
		}
	}

	string columns_json = "[";
	for (idx_t i = 0; i < base.columns.LogicalColumnCount(); i++) {
		auto &col = base.columns.GetColumn(LogicalIndex(i));
		if (i > 0) {
			columns_json += ",";
		}
		columns_json += "{\"name\":\"" + CappedEscapeJson(col.Name().GetIdentifierName()) + "\",\"type\":\"" +
		                CappedEscapeJson(col.Type().ToString()) + "\"}";
	}
	columns_json += "]";

	auto policy_json = PolicySpecFromOptions(base);
	auto manifest = CappedCall(
	    [&](char **out, char **err) {
		    return capped_new_manifest(base.GetTableName().GetIdentifierName().c_str(), columns_json.c_str(),
		                               policy_json.c_str(), out, err);
	    },
	    "CREATE TABLE " + base.GetTableName().GetIdentifierName());
	storage.WriteManifest(fs, base.GetTableName().GetIdentifierName(), manifest);
	entries.erase(StringUtil::Lower(base.GetTableName().GetIdentifierName()));
	return LoadEntry(base.GetTableName().GetIdentifierName());
}

void CappedSchema::DropEntry(ClientContext &context, DropInfo &info) {
	if (catalog.Cast<CappedCatalog>().ReadOnly()) {
		throw IOException("capped: cannot drop entries: the store is attached READ_ONLY");
	}
	if (info.type != CatalogType::TABLE_ENTRY) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw NotImplementedException("capped: DROP of non-table entries is not supported");
	}
	auto &fs = FS();
	string dir;
	for (auto &candidate : storage.ListTables(fs)) {
		if (StringUtil::CIEquals(candidate, info.GetQualifiedName().Name().GetIdentifierName())) {
			dir = candidate;
			break;
		}
	}
	if (dir.empty()) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw CatalogException("capped: table '%s' does not exist",
		                       info.GetQualifiedName().Name().GetIdentifierName());
	}
	// DROP must not interleave with enforcement: a tick that read the
	// manifest before removal would otherwise recreate the directory
	auto lock = catalog.Cast<CappedCatalog>().LockWrites();
	storage.RemoveTable(fs, dir);
	entries.erase(StringUtil::Lower(dir));
}

// --- not (yet) part of the capped model -----------------------------------

optional_ptr<CatalogEntry> CappedSchema::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                     TableCatalogEntry &table) {
	throw NotImplementedException("capped: indexes are not supported");
}
optional_ptr<CatalogEntry> CappedSchema::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw NotImplementedException("capped: user-defined functions are not supported");
}
optional_ptr<CatalogEntry> CappedSchema::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw NotImplementedException("capped: views are not supported");
}
optional_ptr<CatalogEntry> CappedSchema::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw NotImplementedException("capped: sequences are not supported");
}
optional_ptr<CatalogEntry> CappedSchema::CreateTableFunction(CatalogTransaction transaction,
                                                             CreateTableFunctionInfo &info) {
	throw NotImplementedException("capped: table functions are not supported");
}
optional_ptr<CatalogEntry> CappedSchema::CreateCopyFunction(CatalogTransaction transaction,
                                                            CreateCopyFunctionInfo &info) {
	throw NotImplementedException("capped: copy functions are not supported");
}
optional_ptr<CatalogEntry> CappedSchema::CreatePragmaFunction(CatalogTransaction transaction,
                                                               CreatePragmaFunctionInfo &info) {
	throw NotImplementedException("capped: pragma functions are not supported");
}
optional_ptr<CatalogEntry> CappedSchema::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
	throw NotImplementedException("capped: collations are not supported");
}
optional_ptr<CatalogEntry> CappedSchema::CreateCoordinateSystem(CatalogTransaction transaction,
                                                                CreateCoordinateSystemInfo &info) {
	throw NotImplementedException("capped: coordinate systems are not supported");
}
optional_ptr<CatalogEntry> CappedSchema::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw NotImplementedException("capped: custom types are not supported");
}
void CappedSchema::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw NotImplementedException("capped: ALTER TABLE is not supported yet");
}

} // namespace duckdb
