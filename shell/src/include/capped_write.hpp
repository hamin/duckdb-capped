#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_operator_states.hpp"

namespace duckdb {

class Connection;
class QueryResult;
class CappedCatalog;
class CappedTableEntry;
class CappedTransaction;
struct CappedBuffer;

int64_t CappedNowMicros();
string CappedQuoteLit(const string &s);   // single-quoted SQL string literal
string CappedQuoteIdent(const string &s); // double-quoted SQL identifier

//! What one segment of a table asked to have indexed for search. Read from the
//! manifest; the column names are validated against the table schema when the
//! table is created, so they always resolve.
struct CappedSearchSpec {
	//! text columns indexed for BM25 full-text search
	vector<string> columns;
	//! fixed-size float vector columns indexed with HNSW
	vector<string> vector_columns;
	//! distance metric passed to vss: l2sq | cosine | ip
	string metric = "l2sq";

	bool Enabled() const {
		return !columns.empty() || !vector_columns.empty();
	}
};

//! Parse a manifest's search configuration (FFI + yyjson).
CappedSearchSpec CappedReadSearchSpec(const string &manifest_json);

//! One manifest segment (from capped_manifest_list_segments).
struct CappedSegmentInfo {
	int64_t id = 0;
	string path; // root-relative
	int64_t rows = 0;
	int64_t bytes = 0;
	//! Event-time bounds; valid only when has_ts (tables without a time_col, or
	//! with non-temporal values, have no event time at all — never 0/epoch).
	bool has_ts = false;
	int64_t min_ts = 0;
	int64_t max_ts = 0;
	bool sealed = false;
	//! Search index of this segment, when one was built (root-relative). A
	//! sealed segment without one is not searchable: either the table asked for
	//! no indexes, or the build failed (see capped_index).
	bool has_index = false;
	string index_file;
};

//! Parse a manifest's segment list (FFI + yyjson).
vector<CappedSegmentInfo> CappedReadSegments(const string &manifest_json);
//! The policy summary JSON (see capped_manifest_policy_json).
string CappedReadPolicy(const string &manifest_json);

//! Parsed form of the policy summary (empty strings / has_*=false when unset).
struct CappedPolicySummary {
	string time_col;
	string evict_order;
	string on_violation;
	bool has_ttl = false;
	int64_t ttl = 0;
	bool has_max_bytes = false;
	int64_t max_bytes = 0;
	bool has_max_rows = false;
	int64_t max_rows = 0;
};
CappedPolicySummary CappedReadPolicySummary(const string &manifest_json);

//! What one enforcement pass did.
struct CappedEnforceSummary {
	idx_t dropped = 0;
	bool sealed = false;
};

//! Read one row of scalar values out of a materialized result (fetches once).
void CappedFillScalars(QueryResult &result, vector<Value> &out, const string &what);

//! Write path: commit-time flushing of insert buffers into segment files,
//! sealing, and eviction enforcement. All manifest read-modify-write cycles
//! go through here under the catalog write lock; scans read manifests without
//! locking (writes are atomic via write-then-rename).
struct CappedWrite {
	//! Column names stored in a parquet file (top-level, as DuckDB sees them).
	//! Used for the time-column probe and to validate adopted segment files:
	//! DuckDB's parquet reader requires one uniform schema across a table's
	//! segments, so a drifted file would make the whole table unreadable.
	static vector<string> ParquetColumns(Connection &con, const string &abs_path);

	//! Stats of a parquet file on disk: rows, compressed bytes, and the
	//! event-time range in microseconds. has_ts is false when the table has no
	//! time_col or no value in it reads as a timestamp — callers must then store
	//! *no* bound at all (0 would look like 1970 and make retention delete the
	//! segment). Throws if time_col is named but nothing in it is temporal.
	static void ComputeSegmentStats(Connection &con, const string &abs_path, const string &time_col,
	                                int64_t &rows, int64_t &bytes, bool &has_ts, int64_t &min_ts, int64_t &max_ts);

	//! Merge one transaction buffer into the table's open segment: write the
	//! merged parquet, apply eviction, publish, then seal. Hard caps
	//! (on_violation => block) are checked against the merged state before
	//! anything is published; on failure nothing lands and the exception
	//! aborts the commit.
	static void FlushBuffer(ClientContext &context, CappedCatalog &catalog, Connection &con, CappedBuffer &buffer);

	//! Eviction (and open-seal) check after out-of-band manifest changes
	//! (capped_add_segment) or on demand (capped_enforce / ATTACH). Throws if
	//! the table is still over a hard cap.
	static CappedEnforceSummary Enforce(CappedCatalog &catalog, const string &table);

	//! Force-seal the open segment (capped_seal), then enforce. Returns false
	//! when there was no open segment.
	static bool ForceSeal(CappedCatalog &catalog, const string &table);
};

//! Insert sink: appends input chunks into the active transaction's buffer;
//! durability happens at commit (CappedWrite::FlushBuffer). Emits the number
//! of inserted rows, like PhysicalInsert.
class CappedInsert final : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::INSERT;

	CappedInsert(PhysicalPlan &physical_plan, CappedTableEntry &entry_p, idx_t estimated_cardinality);

	CappedTableEntry &entry;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true; // the transaction buffer has its own lock
	}
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;

	bool IsSource() const override {
		return true;
	}
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
