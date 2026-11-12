#include "capped_transaction.hpp"

#include "capped_catalog.hpp"
#include "capped_table.hpp"
#include "capped_write.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/column_definition.hpp"

#include "yyjson.hpp"

#include <mutex>

namespace duckdb {

namespace {
//! Guards transaction buffers against concurrent sinks (parallel insert) and
//! against flushes racing with appends. Process-wide: contention is per
//! statement batch, negligible.
std::mutex buffer_mutex;
} // namespace

string CappedBuffer::BufferTableName() const {
	return "capped_buf_" + table;
}

CappedTransaction::CappedTransaction(TransactionManager &manager, ClientContext &context, transaction_t id)
    : Transaction(manager, context), txn_id(id) {
}

CappedTransaction::~CappedTransaction() {
	// destroy appenders before the connection they write through
	buffers.clear();
	work_con.reset();
}

CappedTransaction &CappedTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog.GetAttached()).Cast<CappedTransaction>();
}

CappedBuffer &CappedTransaction::GetBuffer(ClientContext &context, CappedTableEntry &entry) {
	auto key = StringUtil::Lower(entry.name.GetIdentifierName());
	auto it = buffers.find(key);
	if (it != buffers.end()) {
		return *it->second;
	}
	auto buffer = make_uniq<CappedBuffer>();
	buffer->table = entry.name.GetIdentifierName();
	for (auto &col : entry.GetColumns().Physical()) {
		buffer->column_names.push_back(col.Name().GetIdentifierName());
		buffer->column_types.push_back(col.Type());
	}
	buffer->rows = 0;

	if (!work_con) {
		work_con = make_uniq<Connection>(DatabaseInstance::GetDatabase(context));
	}
	string ddl = "CREATE TEMP TABLE IF NOT EXISTS " + CappedQuoteIdent(buffer->BufferTableName()) + "(";
	for (idx_t i = 0; i < buffer->column_names.size(); i++) {
		if (i > 0) {
			ddl += ", ";
		}
		ddl += CappedQuoteIdent(buffer->column_names[i]) + " " + buffer->column_types[i].ToString();
	}
	ddl += ")";
	auto create = work_con->Query(ddl);
	if (create->HasError()) {
		throw IOException("capped: creating insert buffer failed: %s", create->GetError());
	}
	buffer->appender =
	    make_uniq<Appender>(*work_con, "temp", "main", Identifier(buffer->BufferTableName()));

	auto *result = buffer.get();
	buffers[key] = std::move(buffer);
	return *result;
}

void CappedTransaction::BufferChunk(ClientContext &context, CappedTableEntry &entry, DataChunk &chunk) {
	std::lock_guard<std::mutex> guard(buffer_mutex);
	if (chunk.ColumnCount() != entry.GetColumns().PhysicalColumnCount()) {
		throw InternalException("capped: insert chunk column count does not match table schema");
	}
	auto &buffer = GetBuffer(context, entry);
	buffer.appender->AppendDataChunk(chunk);
	buffer.rows += chunk.size();
}

bool CappedTransaction::HasBufferFor(const string &table) const {
	return buffers.count(StringUtil::Lower(table)) > 0;
}

void CappedTransaction::FlushAll(ClientContext &context) {
	std::lock_guard<std::mutex> guard(buffer_mutex);
	if (buffers.empty()) {
		return;
	}
	auto &catalog = Catalog::GetCatalog(manager.GetDB()).Cast<CappedCatalog>();
	for (auto &entry : buffers) {
		CappedWrite::FlushBuffer(context, catalog, *work_con, *entry.second);
	}
	buffers.clear();
	work_con.reset();
}

void CappedTransaction::DiscardAll() {
	std::lock_guard<std::mutex> guard(buffer_mutex);
	buffers.clear();
	work_con.reset();
}

CappedTransactionManager::CappedTransactionManager(AttachedDatabase &db, CappedStorageInfo storage)
    : TransactionManager(db), storage(std::move(storage)) {
}

Transaction &CappedTransactionManager::StartTransaction(ClientContext &context) {
	auto txn = make_uniq<CappedTransaction>(*this, context, next_id.fetch_add(1));
	auto &ref = *txn;
	std::lock_guard<std::mutex> guard(active_lock);
	active.emplace(ref.txn_id, std::move(txn));
	return ref;
}

ErrorData CappedTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	ErrorData error;
	try {
		transaction.Cast<CappedTransaction>().FlushAll(context);
	} catch (std::exception &ex) {
		error = ErrorData(ex);
	}
	RemoveTransaction(transaction);
	return error;
}

void CappedTransactionManager::RollbackTransaction(Transaction &transaction) {
	transaction.Cast<CappedTransaction>().DiscardAll();
	RemoveTransaction(transaction);
}

void CappedTransactionManager::RemoveTransaction(Transaction &transaction) {
	auto id = transaction.Cast<CappedTransaction>().txn_id;
	std::lock_guard<std::mutex> guard(active_lock);
	active.erase(id);
}

void CappedTransactionManager::Checkpoint(ClientContext &context, bool force) {
	// no WAL; segment files are the durable unit
}

} // namespace duckdb
