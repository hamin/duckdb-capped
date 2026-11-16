#include "capped_index.hpp"

#include "capped.h"
#include "capped_catalog.hpp"
#include "capped_storage_info.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/connection.hpp"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstring>

namespace duckdb {

// --- how a sidecar is laid out ---------------------------------------------
//
// One .duckdb file per sealed segment, next to it: logs/2.seg.parquet becomes
// logs/2.seg.idx.duckdb and holds a single table idx_2 with
//
//   _rid     BIGINT   row_number() at build time: a stable positional handle
//   <text>   VARCHAR  the search_columns, for BM25
//   <vector> FLOAT[N] the vector_columns, for HNSW
//
// Non-text search columns (validated display-castable by the core, e.g. a
// log's TIMESTAMP) are also stored here as VARCHAR — carried so a hit renders
// without touching the segment, but left out of the BM25 index.
//
// plus fts_main_idx_2 (the index schema fts creates) and one hnsw index per
// vector column. Row ids are reproducible because DuckDB reads a parquet file
// in a fixed order, which is what makes a hit addressable later.

namespace {

string SidecarTable(int64_t segment_id) {
	// deliberately lower-case and underscore-only: fts names its index schema
	// fts_main_<table> and a query has to spell that name out
	return "idx_" + to_string(segment_id);
}

string SidecarAlias(int64_t segment_id) {
	return "capped_idx_" + to_string(segment_id);
}

bool TryQuery(Connection &con, const string &sql, string &error, const string &what) {
	auto result = con.Query(sql);
	if (!result || result->HasError()) {
		error = "capped: " + what + " failed: " + (result ? result->GetError() : string("no result"));
		return false;
	}
	return true;
}

//! StringUtil::Trim works in place, which is awkward for the many places here
//! that want a trimmed *copy* of something already const
string Trimmed(const string &value) {
	auto copy = value;
	StringUtil::Trim(copy);
	return copy;
}

//! Does the manifest declare this as a text type fts can tokenize? Non-text
//! search columns are validated as display-castable by the core and arrive
//! here already cast to VARCHAR; they render in hits but are not indexed.
bool IsTextType(const string &type) {
	auto t = Trimmed(type);
	StringUtil::Upper(t);
	return t == "VARCHAR" || t == "TEXT" || t == "STRING";
}

string ScalarString(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	if (!result || result->HasError()) {
		return string();
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() != 1) {
		return string();
	}
	return chunk->GetValue(0, 0).ToString();
}

idx_t FileSizeOnDisk(FileSystem &fs, const string &path) {
	try {
		auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
		if (!handle) {
			return 0;
		}
		return NumericCast<idx_t>(fs.GetFileSize(*handle));
	} catch (...) { // a missing or unreadable index is overhead we don't report
		return 0;
	}
}

//! One searchable column of a table, as the manifest spells it.
struct SearchColumn {
	string name;
	string type; // declared type: VARCHAR, FLOAT[8], ...
	bool vector_column = false;
};

vector<SearchColumn> SearchColumns(const CappedSearchSpec &spec, const string &manifest_json) {
	CappedString err;
	int64_t count = 0;
	if (capped_manifest_column_count(manifest_json.c_str(), &count, err.Receive()) != CAPPED_OK) {
		throw IOException("capped: reading manifest columns failed: %s", err.Str());
	}
	auto find_type = [&](const string &name) -> string {
		for (int64_t i = 0; i < count; i++) {
			CappedString n, t;
			if (capped_manifest_column(manifest_json.c_str(), i, n.Receive(), t.Receive(), err.Receive()) !=
			    CAPPED_OK) {
				throw IOException("capped: reading manifest column %lld failed: %s", i, err.Str());
			}
			if (StringUtil::CIEquals(n.Str(), name)) {
				return t.Str();
			}
		}
		return string();
	};
	vector<SearchColumn> out;
	for (auto &kind : {false, true}) {
		auto &names = kind ? spec.vector_columns : spec.columns;
		for (auto &name : names) {
			SearchColumn col;
			col.name = name;
			col.vector_column = kind;
			col.type = find_type(name);
			if (col.type.empty()) {
				// names are validated against the schema at CREATE TABLE and there
				// is no ALTER TABLE, so this is corruption rather than a typo
				throw IOException("capped: table indexes column '%s' that the manifest does not have", name);
			}
			out.push_back(std::move(col));
		}
	}
	return out;
}

// --- building --------------------------------------------------------------

//! Build one segment's sidecar from scratch. On failure nothing is left behind:
//! the segment simply stays unsearchable, because an index is never worth
//! failing a write (or taking down an enforce sweep) over.
bool BuildSegmentIndex(CappedCatalog &catalog, const CappedSegmentInfo &seg, const vector<SearchColumn> &columns,
                       const string &metric, string &index_rel, string &error) {
	auto &fs = catalog.FS();
	auto &storage = catalog.Storage();
	index_rel = CappedIndex::IndexPathFor(seg.path);
	auto index_abs = storage.Root() + "/" + index_rel;
	auto segment_abs = storage.Root() + "/" + seg.path;

	auto fail = [&](const string &message) {
		error = message;
		CappedIndex::Remove(catalog, index_rel); // a partial build is not an index
		return false;
	};
	// for failures where TryQuery already filled in `error`
	auto bail = [&]() {
		CappedIndex::Remove(catalog, index_rel);
		return false;
	};

	if (!fs.FileExists(segment_abs)) {
		return fail("capped: cannot index '" + seg.path + "': the segment file is gone");
	}
	// never trust a leftover: a crashed build or a segment replaced under the
	// same id must not survive as someone else's index
	CappedIndex::Remove(catalog, index_rel);

	bool wants_text = false, wants_vectors = false;
	for (auto &col : columns) {
		if (col.vector_column) {
			wants_vectors = true;
		} else {
			wants_text = true;
		}
	}
	auto alias = SidecarAlias(seg.id);
	auto table = SidecarTable(seg.id);

	// own connection: a failed statement poisons the transaction it ran in, and
	// a best-effort index build must never damage the caller's
	Connection con(catalog.GetDatabase());
	auto original_db = ScalarString(con, "SELECT current_database()");
	auto leave = [&]() {
		// cannot detach the current database, so go back first
		if (!original_db.empty()) {
			con.Query("USE " + CappedQuoteIdent(original_db));
		}
		con.Query("DETACH " + CappedQuoteIdent(alias));
	};

	if (wants_text && !TryQuery(con, "LOAD fts", error, "LOAD fts (needed for full-text search)")) {
		return bail();
	}
	if (wants_vectors && !TryQuery(con, "LOAD vss", error, "LOAD vss (needed for vector search)")) {
		return bail();
	}
	if (wants_vectors && !TryQuery(con, "SET hnsw_enable_experimental_persistence=true", error,
	                              "allowing persistent HNSW indexes")) {
		return bail();
	}
	// an empty duckdb file costs ~0.5 MB at the default block size, which a small
	// segment would pay per index file; 16k is the floor duckdb accepts and takes
	// an fts sidecar from 1.8 MB to 190 KB. Only for text: hnsw builds a
	// FixedSizeAllocator over whole blocks and needs the standard 256k block, so a
	// smaller one throws an INTERNAL error that invalidates the entire database --
	// far too expensive for a best-effort index
	string block_size = wants_vectors ? "" : " (BLOCK_SIZE 16384)";
	if (!TryQuery(con, "ATTACH " + CappedQuoteLit(index_abs) + " AS " + CappedQuoteIdent(alias) + block_size, error,
	              "creating the search index file")) {
		leave();
		return bail();
	}
	// fts names and pragmas resolve against the *current* database, so the
	// sidecar has to be it while it is being built
	if (!TryQuery(con, "USE " + CappedQuoteIdent(alias), error, "selecting the search index")) {
		leave();
		return bail();
	}

	string select_list;
	for (auto &col : columns) {
		if (!select_list.empty()) {
			select_list += ", ";
		}
		select_list += CappedQuoteIdent(col.name);
		if (col.vector_column) {
			// parquet round-trips a fixed-size list as a plain list, and vss only
			// indexes FLOAT[N], so the declared shape goes back in explicitly --
			// and the cast has to be named back, or the column lands as CAST(...)
			// and CREATE INDEX cannot find it
			select_list += "::" + col.type + " AS " + CappedQuoteIdent(col.name);
		} else if (!IsTextType(col.type)) {
			// display-only search column (e.g. a log's timestamp): carried through
			// as text so a hit renders on its own, but kept out of the BM25
			// pragma below so its digits never pollute relevance
			select_list += "::VARCHAR AS " + CappedQuoteIdent(col.name);
		}
	}
	auto create = "CREATE TABLE " + CappedQuoteIdent(table) + " AS SELECT row_number() OVER () AS _rid, " +
	              select_list + " FROM read_parquet(" + CappedQuoteLit(segment_abs) + ")";
	if (!TryQuery(con, create, error, "materializing the search index of '" + seg.path + "'")) {
		leave();
		return bail();
	}

	if (wants_text) {
		string cols;
		for (auto &col : columns) {
			if (!col.vector_column && IsTextType(col.type)) {
				cols += ", " + CappedQuoteLit(col.name);
			}
		}
		auto pragma = "PRAGMA create_fts_index(" + CappedQuoteLit(table) + ", " + CappedQuoteLit("_rid") + cols +
		              ", overwrite=1)";
		if (!TryQuery(con, pragma, error, "building the full-text index of '" + seg.path + "'")) {
			leave();
			return bail();
		}
	}
	idx_t vector_ordinal = 0;
	for (auto &col : columns) {
		if (!col.vector_column) {
			continue;
		}
		auto index_name = "hnsw_" + to_string(seg.id) + "_" + to_string(vector_ordinal++);
		auto sql = "CREATE INDEX " + CappedQuoteIdent(index_name) + " ON " + CappedQuoteIdent(table) + " USING HNSW (" +
		           CappedQuoteIdent(col.name) + ") WITH (metric=" + CappedQuoteLit(metric) + ")";
		if (!TryQuery(con, sql, error, "building the HNSW index on '" + col.name + "' of '" + seg.path + "'")) {
			leave();
			return bail();
		}
	}

	leave();
	return true;
}

string RecordSegmentIndex(const string &manifest_json, int64_t segment_id, const string &index_rel) {
	return CappedCall(
	    [&](char **out, char **err) {
		    return capped_manifest_set_segment_index(manifest_json.c_str(), segment_id, index_rel.c_str(), out, err);
	    },
	    "recording the search index of segment " + to_string(segment_id));
}

// --- searching -------------------------------------------------------------

struct SearchTarget {
	int64_t segment_id = 0;
	string index_rel;
};

//! Sealed segments that actually have an index on disk right now. Search covers
//! exactly these, so eviction makes hits disappear with the data itself.
vector<SearchTarget> LiveIndexedSegments(CappedCatalog &catalog, const string &manifest_json) {
	vector<SearchTarget> out;
	auto &fs = catalog.FS();
	auto &storage = catalog.Storage();
	for (auto &seg : CappedReadSegments(manifest_json)) {
		if (!seg.sealed || !seg.has_index) {
			continue;
		}
		if (!fs.FileExists(storage.Root() + "/" + seg.index_file)) {
			continue; // hand-deleted, or a build that died halfway
		}
		SearchTarget target;
		target.segment_id = seg.id;
		target.index_rel = seg.index_file;
		out.push_back(std::move(target));
	}
	return out;
}

//! Run one per-segment query against its sidecar and collect the rows. The
//! builder gets the alias and table name because fts's match_bm25 only resolves
//! against the current database, hence the USE and the one-segment-per-query
//! shape; merging the per-segment top-k lists happens in the caller.
//! What a per-segment query needs from the extension world. Full-text search
//! cannot run at all without fts (its plan calls stem()), so a missing extension
//! is an error there; vector search is exact-distance and only uses hnsw when vss
//! happens to be loaded, so there losing the extension is a slowdown, not a failure.
enum class SidecarSearch { Text, Vector };

bool ForEachSidecarRow(CappedCatalog &catalog, const SearchTarget &target, SidecarSearch kind,
                       const std::function<string(const string &alias, const string &table)> &sql_for,
                       vector<CappedSearchHit> &hits, string &error) {
	auto abs_path = catalog.Storage().Root() + "/" + target.index_rel;
	auto alias = SidecarAlias(target.segment_id);
	auto table = SidecarTable(target.segment_id);

	Connection con(catalog.GetDatabase());
	auto original_db = ScalarString(con, "SELECT current_database()");

	// the sidecar's own plan can call into fts/vss, which only exist once the
	// extension is loaded in this connection too -- and it has to happen before
	// USE, while the connection still points at a database that can load
	string ext = kind == SidecarSearch::Text ? "fts" : "vss";
	if (!TryQuery(con, "LOAD " + ext, error, "LOAD " + ext + " (needed to search '" + target.index_rel + "')")) {
		if (kind == SidecarSearch::Text) {
			return false;
		}
		error.clear(); // exact kNN still answers, just without the index
	}

	// read-only where possible: searches are concurrent and a read-write attach
	// would fight over the file lock
	auto attach = "ATTACH " + CappedQuoteLit(abs_path) + " AS " + CappedQuoteIdent(alias);
	auto result = con.Query(attach + " (READ_ONLY)");
	if (!result || result->HasError()) {
		// a sidecar whose build died mid-way still has a WAL to replay, which a
		// read-only attach refuses: retry writable rather than lose the segment
		result = con.Query(attach);
		if (!result || result->HasError()) {
			error = "capped: opening the search index of segment " + to_string(target.segment_id) +
			        " ('" + target.index_rel + "') failed: " +
			        (result ? result->GetError() : string("no result"));
			return false;
		}
	}
	if (!TryQuery(con, "USE " + CappedQuoteIdent(alias), error, "selecting the search index of segment " +
	                                                                    to_string(target.segment_id))) {
		return false;
	}
	auto rows = con.Query(sql_for(alias, table));
	if (!rows || rows->HasError()) {
		error = "capped: searching segment " + to_string(target.segment_id) + " failed: " +
		        (rows ? rows->GetError() : string("no result"));
	} else {
		while (auto chunk = rows->Fetch()) {
			auto ncols = chunk->ColumnCount();
			if (ncols < 2) {
				error = "capped: internal error, search query returned fewer than two columns";
				break;
			}
			for (idx_t r = 0; r < chunk->size(); r++) {
				CappedSearchHit hit;
				hit.segment_id = target.segment_id;
				hit.row_id = chunk->GetValue(0, r).GetValue<int64_t>();
				hit.score = chunk->GetValue(ncols - 1, r).GetValue<double>();
				for (idx_t c = 1; c + 1 < ncols; c++) {
					hit.values.push_back(chunk->GetValue(c, r));
				}
				hits.push_back(std::move(hit));
			}
		}
	}
	if (!original_db.empty()) {
		con.Query("USE " + CappedQuoteIdent(original_db));
	}
	con.Query("DETACH " + CappedQuoteIdent(alias));
	return error.empty();
}

int64_t ClampK(int64_t k) {
	if (k <= 0) {
		return 10; // a sane default beats an empty answer or a full ranking
	}
	return k;
}

} // namespace

// --- public surface --------------------------------------------------------

string CappedIndex::IndexPathFor(const string &segment_path) {
	return CappedCall(
	    [&](char **out, char **err) { return capped_index_path_for_segment(segment_path.c_str(), out, err); },
	    "computing the index path of '" + segment_path + "'");
}

void CappedIndex::Remove(CappedCatalog &catalog, const string &rel_index_path) {
	if (rel_index_path.empty()) {
		return;
	}
	auto &fs = catalog.FS();
	auto abs_path = catalog.Storage().Root() + "/" + rel_index_path;
	try {
		// vss/fts can leave a WAL behind; both files are the index, nothing else
		for (auto &path : {abs_path, abs_path + ".wal"}) {
			if (fs.FileExists(path)) {
				fs.RemoveFile(path);
			}
		}
	} catch (...) { // best effort: an index that survives is only wasted space
	}
}

idx_t CappedIndex::BytesOnDisk(CappedCatalog &catalog, const string &rel_index_path) {
	if (rel_index_path.empty()) {
		return 0;
	}
	return FileSizeOnDisk(catalog.FS(), catalog.Storage().Root() + "/" + rel_index_path);
}

idx_t CappedIndex::EnsureUnlocked(CappedCatalog &catalog, const string &table, string &error, CappedIndexMode mode) {
	if (catalog.ReadOnly()) {
		return 0; // index files are store writes; a READ_ONLY attach builds none
	}
	auto &fs = catalog.FS();
	auto &storage = catalog.Storage();
	auto manifest = storage.ReadManifest(fs, table);
	auto spec = CappedReadSearchSpec(manifest);
	if (!spec.Enabled()) {
		return 0; // the table never asked for search
	}
	if (mode == CappedIndexMode::BuildMissing && catalog.SearchDegraded()) {
		// a missing fts/vss extension would otherwise be re-loaded on every
		// commit; capped_index() or a re-ATTACH tries again
		return 0;
	}
	auto columns = SearchColumns(spec, manifest);
	auto segments = CappedReadSegments(manifest);

	idx_t built = 0;
	for (auto &seg : segments) {
		if (!seg.sealed) {
			continue; // the open segment is rewritten on every commit: index it once sealed
		}
		if (seg.has_index && mode != CappedIndexMode::Rebuild) {
			if (fs.FileExists(storage.Root() + "/" + seg.index_file)) {
				continue;
			}
		}
		string index_rel;
		if (!BuildSegmentIndex(catalog, seg, columns, spec.metric, index_rel, error)) {
			catalog.SetSearchDegraded(true);
			return built; // one unwritable segment must not hide the rest of the store
		}
		manifest = RecordSegmentIndex(manifest, seg.id, index_rel);
		storage.WriteManifest(fs, table, manifest);
		built++;
	}
	if (built || mode != CappedIndexMode::BuildMissing) {
		catalog.SetSearchDegraded(false);
	}
	return built;
}

idx_t CappedIndex::Ensure(CappedCatalog &catalog, const string &table, string &error, CappedIndexMode mode) {
	auto lock = catalog.LockWrites();
	return EnsureUnlocked(catalog, table, error, mode);
}

CappedSearchResult CappedIndex::SearchText(CappedCatalog &catalog, const string &table, const string &query,
                                           int64_t k) {
	auto manifest = catalog.Storage().ReadManifest(catalog.FS(), table);
	auto spec = CappedReadSearchSpec(manifest);
	if (spec.columns.empty()) {
		throw BinderException(
		    "capped: table '%s' has no full-text search columns; create it WITH (search_columns='...')", table);
	}
	if (Trimmed(query).empty()) {
		throw BinderException("capped: capped_fts needs a non-empty query");
	}
	auto columns = SearchColumns(spec, manifest);
	auto targets = LiveIndexedSegments(catalog, manifest);
	if (targets.empty()) {
		throw IOException("capped: table '%s' has no searchable sealed segments yet (capped_index('%s','%s') builds "
		                  "them; the open segment is not indexed)",
		                  table, catalog.GetName(), table);
	}

	CappedSearchResult result;
	for (auto &col : columns) {
		if (col.vector_column) {
			continue; // a result list wants text, not a vector
		}
		result.names.push_back(col.name);
		result.types.push_back(LogicalType::VARCHAR);
	}
	auto limit = ClampK(k);
	vector<CappedSearchHit> hits;
	string error;
	for (auto &target : targets) {
		auto sql_for = [&](const string &alias, const string &tbl) {
			string cols;
			for (auto &col : columns) {
				if (!col.vector_column) {
					cols += ", " + CappedQuoteIdent(col.name);
				}
			}
			return "SELECT _rid" + cols + ", score FROM (SELECT _rid" + cols + ", fts_main_" + tbl +
			       ".match_bm25(_rid, " + CappedQuoteLit(query) + ")::DOUBLE AS score FROM " + CappedQuoteIdent(alias) + "." +
			       CappedQuoteIdent(tbl) + ") WHERE score IS NOT NULL ORDER BY score DESC LIMIT " + to_string(limit);
		};
		if (!ForEachSidecarRow(catalog, target, SidecarSearch::Text, sql_for, hits, error)) {
			throw IOException("%s (rebuild it with capped_index('%s','%s', rebuild => true))", error,
			                  catalog.GetName(), table);
		}
	}
	// each segment gave its own best `limit`: merge, then take the table-wide top-k
	std::sort(hits.begin(), hits.end(), [](const CappedSearchHit &a, const CappedSearchHit &b) {
		return a.score > b.score;
	});
	if (hits.size() > idx_t(limit)) {
		hits.resize(idx_t(limit));
	}
	result.hits = std::move(hits);
	return result;
}

CappedSearchResult CappedIndex::SearchVector(CappedCatalog &catalog, const string &table, const string &column,
                                             const Value &query_vector, int64_t k) {
	auto manifest = catalog.Storage().ReadManifest(catalog.FS(), table);
	auto spec = CappedReadSearchSpec(manifest);
	if (spec.vector_columns.empty()) {
		throw BinderException("capped: table '%s' has no vector columns; create it WITH (vector_columns='...')",
		                      table);
	}
	auto columns = SearchColumns(spec, manifest);

	// which vector column: named, or the only one there is
	auto wanted = Trimmed(column);
	const SearchColumn *vector_col = nullptr;
	if (wanted.empty() && spec.vector_columns.size() > 1) {
		string names;
		for (auto &name : spec.vector_columns) {
			names += (names.empty() ? "" : ", ") + name;
		}
		throw BinderException("capped: table '%s' indexes several vector columns (%s); pass one as column => '...'",
		                      table, names);
	}
	for (auto &col : columns) {
		if (col.vector_column && (wanted.empty() || StringUtil::CIEquals(col.name, wanted))) {
			vector_col = &col;
			break;
		}
	}
	if (!vector_col) {
		throw BinderException("capped: '%s' is not an indexed vector column of table '%s'", column, table);
	}
	// the HNSW index only matches a FLOAT[N] operand of the same width, so the
	// declared shape is parsed out of the column type and put back on the literal
	auto declared = StringUtil::Upper(Trimmed(vector_col->type));
	auto open = declared.find('[');
	if (open == string::npos || !StringUtil::EndsWith(declared, "]")) {
		throw IOException("capped: vector column '%s' has type '%s', which cannot hold an HNSW index",
		                  vector_col->name, vector_col->type);
	}
	auto dims = Trimmed(declared.substr(open + 1, declared.size() - open - 2));
	int64_t dim = 0;
	try {
		dim = std::stoll(dims);
	} catch (...) {
		throw IOException("capped: vector column '%s' has type '%s', which is not a fixed-size float array",
		                  vector_col->name, vector_col->type);
	}

	if (query_vector.IsNull()) {
		throw BinderException("capped: capped_vector_search needs a query vector, got NULL");
	}
	if (dim <= 0) {
		throw IOException("capped: vector column '%s' is declared %s, which cannot be searched",
		                  vector_col->name, vector_col->type);
	}
	auto kind = query_vector.type().id();
	if (kind != LogicalTypeId::LIST && kind != LogicalTypeId::ARRAY) {
		throw BinderException("capped: capped_vector_search needs a list of %d floats, got %s", (int)dim,
		                      query_vector.type().ToString());
	}
	auto &children =
	    kind == LogicalTypeId::LIST ? ListValue::GetChildren(query_vector) : ArrayValue::GetChildren(query_vector);
	if (int64_t(children.size()) != dim) {
		throw BinderException("capped: vector column '%s' is %s; the query vector has %d value(s)",
		                      vector_col->name, vector_col->type, (int)children.size());
	}
	string literal = "[";
	for (idx_t i = 0; i < children.size(); i++) {
		auto element = children[i];
		if (!element.DefaultTryCastAs(LogicalType::DOUBLE)) {
			throw BinderException("capped: the query vector has a non-numeric value at position %d", (int)i + 1);
		}
		auto value = element.GetValue<double>();
		if (!std::isfinite(value)) {
			throw BinderException("capped: the query vector has a non-finite value at position %d", (int)i + 1);
		}
		if (i) {
			literal += ", ";
		}
		char buf[32];
		// 9 significant digits round-trips a float32 exactly, which is what the
		// FLOAT[N] operand of the HNSW search holds
		std::snprintf(buf, sizeof(buf), "%.9g", value);
		literal += buf;
	}
	literal += "]::FLOAT[" + to_string(dim) + "]";

	// vss ranks by distance for l2sq/cosine and by raw inner product for ip, so
	// "nearest" means ASC for the first two and DESC for the last one
	const char *function = "array_distance";
	const char *direction = "ASC";
	if (spec.metric == "cosine") {
		function = "array_cosine_distance";
	} else if (spec.metric == "ip") {
		function = "array_inner_product";
		direction = "DESC";
	}

	CappedSearchResult result;
	for (auto &col : columns) {
		if (col.vector_column) {
			continue;
		}
		result.names.push_back(col.name);
		result.types.push_back(LogicalType::VARCHAR);
	}
	auto targets = LiveIndexedSegments(catalog, manifest);
	if (targets.empty()) {
		throw IOException("capped: table '%s' has no searchable sealed segments yet (capped_index('%s','%s') builds "
		                  "them; the open segment is not indexed)",
		                  table, catalog.GetName(), table);
	}
	auto limit = ClampK(k);
	vector<CappedSearchHit> hits;
	string error;
	for (auto &target : targets) {
		auto sql_for = [&](const string &alias, const string &tbl) {
			string cols;
			for (auto &col : columns) {
				if (!col.vector_column) {
					cols += ", " + CappedQuoteIdent(col.name);
				}
			}
			return string("SELECT _rid") + cols + ", " + function + "(" + CappedQuoteIdent(vector_col->name) + ", " +
			       literal + ")::DOUBLE AS score FROM " + CappedQuoteIdent(alias) + "." + CappedQuoteIdent(tbl) +
			       " ORDER BY score " + direction + " LIMIT " + to_string(limit);
		};
		if (!ForEachSidecarRow(catalog, target, SidecarSearch::Vector, sql_for, hits, error)) {
			throw IOException("%s (rebuild it with capped_index('%s','%s', rebuild => true))", error,
			                  catalog.GetName(), table);
		}
	}
	std::sort(hits.begin(), hits.end(), [&](const CappedSearchHit &a, const CappedSearchHit &b) {
		return strcmp(direction, "DESC") == 0 ? a.score > b.score : a.score < b.score;
	});
	if (hits.size() > idx_t(limit)) {
		hits.resize(idx_t(limit));
	}
	result.hits = std::move(hits);
	return result;
}

} // namespace duckdb
