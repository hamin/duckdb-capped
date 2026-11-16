#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"

#include "capped_write.hpp"

namespace duckdb {

class Connection;
class CappedCatalog;

//! One search hit: which sealed segment it came from, the row's position inside
//! that segment, the searchable column values (in CappedSearchResult::names
//! order, excluding the vector itself), and the ranking value — a BM25 score for
//! text, or the configured metric's value for vectors.
struct CappedSearchHit {
	int64_t segment_id = 0;
	//! 1-based position of the row within its segment (row_number() at build time)
	int64_t row_id = 0;
	vector<Value> values;
	double score = 0;
};

struct CappedSearchResult {
	//! searchable columns carried back with every hit
	vector<string> names;
	vector<LogicalType> types;
	vector<CappedSearchHit> hits;
};

//! How hard to try when building indexes.
enum class CappedIndexMode : uint8_t {
	//! the write path: only what is missing, and nothing at all after an earlier
	//! build in this store failed (so a missing extension is not retried per commit)
	BuildMissing,
	//! ATTACH: retry after a failure, e.g. because fts/vss got installed meanwhile
	Retry,
	//! capped_index(rebuild => true): throw away and rebuild every index
	Rebuild
};

//! Search indexes over sealed segments.
//!
//! A segment is a Parquet file read by DuckDB's own reader — which is what makes
//! analytics fast, but it carries no inverted or vector index, so
//! `body LIKE '%timeout%'` over retained telemetry is a text scan of every
//! segment. A table with search columns gets, per sealed segment, a sibling
//! `<id>.seg.idx.duckdb` holding just the searchable columns plus a BM25 fts
//! index and HNSW vector indexes (the `fts` and `vss` extensions).
//!
//! The index is derived data: built when a segment seals, deleted with it, and
//! rebuildable at any time with capped_index. Nothing in the write or scan path
//! depends on it — a failed build never fails the write, the segment just
//! reports `indexed = false`. Retention caps count segment bytes only, so
//! adding an index must not change what eviction decides.
//!
//! Queries run one sidecar at a time: `fts`'s match_bm25 resolves against the
//! current database only, so cross-segment qualification is impossible and the
//! per-segment top-k lists are merged here instead.
struct CappedIndex {
	//! Store-relative path of the index belonging to a segment file. The
	//! convention lives in capped-core so writer and reader cannot drift.
	static string IndexPathFor(const string &segment_path);

	//! Delete a segment's index (and a leftover WAL). Best effort.
	static void Remove(CappedCatalog &catalog, const string &rel_index_path);

	//! Disk footprint of an index file (0 when absent): overhead we report but
	//! deliberately never count against the retention caps.
	static idx_t BytesOnDisk(CappedCatalog &catalog, const string &rel_index_path);

	//! Build every missing index of the table's sealed segments. The caller holds
	//! the catalog write lock. Returns how many indexes were built; `error`
	//! carries the reason for the first failure (which latches further write-path
	//! attempts until a Retry or Rebuild run succeeds).
	static idx_t EnsureUnlocked(CappedCatalog &catalog, const string &table, string &error, CappedIndexMode mode);

	//! Same, taking the write lock.
	static idx_t Ensure(CappedCatalog &catalog, const string &table, string &error, CappedIndexMode mode);

	//! BM25 top-k over the table's indexed sealed segments.
	static CappedSearchResult SearchText(CappedCatalog &catalog, const string &table, const string &query,
	                                     int64_t k);

	//! HNSW top-k over the table's indexed sealed segments. `column` may be empty
	//! when the table has exactly one vector column; `query_vector` is a list or
	//! array of floats of the column's declared dimension.
	static CappedSearchResult SearchVector(CappedCatalog &catalog, const string &table, const string &column,
	                                       const Value &query_vector, int64_t k);
};

} // namespace duckdb
