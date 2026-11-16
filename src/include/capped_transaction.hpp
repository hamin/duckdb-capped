#pragma once

#include "capped_storage_info.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

#include <atomic>
#include <mutex>

namespace duckdb {

class ClientContext;
class Connection;
class Appender;
class CappedCatalog;
class CappedTableEntry;
class DataChunk;

//! One table's uncommitted insert buffer: rows are appended (vectorized) to a
//! temp table on a transaction-owned internal connection; the commit path
//! merges that temp table into the table's open segment file. Rollback just
//! drops the connection, so uncommitted data never touches the store.
struct CappedBuffer {
	string table;
	//! Schema snapshot taken when the buffer was created (used at flush time).
	vector<string> column_names;
	vector<LogicalType> column_types;
	unique_ptr<Appender> appender;
	idx_t rows = 0;

	string BufferTableName() const;
};

class CappedTransaction : public Transaction {
public:
	CappedTransaction(TransactionManager &manager, ClientContext &context, transaction_t id);
	~CappedTransaction() override;

	static CappedTransaction &Get(ClientContext &context, Catalog &catalog);

	//! Id assigned by the manager; its key in the manager's active set.
	const transaction_t txn_id;

	//! Append rows to a table's buffer, creating the buffer/temp table on
	//! demand.
	void BufferChunk(ClientContext &context, CappedTableEntry &entry, DataChunk &chunk);

	bool HasBuffers() const {
		return !buffers.empty();
	}
	//! True when this transaction has uncommitted (buffered) rows for the
	//! table: reading it back before commit would silently miss them, so the
	//! scan path refuses with a clear error instead.
	bool HasBufferFor(const string &table) const;
	//! Merge every buffer into its table's open segment + run enforcement
	//! (seal / evict / block). Throws on failure; the commit is aborted then.
	void FlushAll(ClientContext &context);
	//! Drop all buffered data (rollback).
	void DiscardAll();

private:
	CappedBuffer &GetBuffer(ClientContext &context, CappedTableEntry &entry);

	unordered_map<string, unique_ptr<CappedBuffer>> buffers; // key: lower-case table name
	unique_ptr<Connection> work_con;
};

class CappedTransactionManager : public TransactionManager {
public:
	CappedTransactionManager(AttachedDatabase &db, CappedStorageInfo storage);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;
	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	void RemoveTransaction(Transaction &transaction);

	CappedStorageInfo storage;
	//! Every live transaction, keyed by its id. DuckDB gives each connection
	//! its own MetaTransaction, so transactions from OTLP ingest, the sealer
	//! and UI readers overlap on one manager: the manager must own every
	//! transaction from StartTransaction until its Commit/Rollback. A single
	//! shared slot freed whichever transaction a different connection started
	//! last — committing the earlier one then read freed buffers (SIGSEGV in
	//! FlushAll under ingest load).
	unordered_map<transaction_t, unique_ptr<CappedTransaction>> active;
	std::mutex active_lock;
	std::atomic<transaction_t> next_id {1};
};

} // namespace duckdb
