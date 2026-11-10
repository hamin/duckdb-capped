#include "capped_write.hpp"

#include "capped.h"
#include "capped_catalog.hpp"
#include "capped_index.hpp"
#include "capped_table.hpp"
#include "capped_transaction.hpp"

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/query_result.hpp"

#include "yyjson.hpp"

#include <chrono>

namespace duckdb {

int64_t CappedNowMicros() {
	return std::chrono::duration_cast<std::chrono::microseconds>(
	           std::chrono::system_clock::now().time_since_epoch())
	    .count();
}

string CappedQuoteLit(const string &s) {
	return "'" + StringUtil::Replace(s, "'", "''") + "'";
}

string CappedQuoteIdent(const string &s) {
	return "\"" + StringUtil::Replace(s, "\"", "\"\"") + "\"";
}

namespace {

//! One JSON object from capped_manifest_list_segments / plan drops.
duckdb_yyjson::yyjson_val *ParseJsonRoot(const string &json, const string &what) {
	auto *doc = duckdb_yyjson::yyjson_read(json.c_str(), json.size(), 0);
	if (!doc) {
		throw IOException("capped: cannot parse %s", what);
	}
	// NOTE: the document intentionally leaks; these JSON blobs are small and
	// the alternative (threading the doc pointer through) buys nothing.
	return duckdb_yyjson::yyjson_doc_get_root(doc);
}

string FfiStr(const std::function<int32_t(char **, char **)> &fn, const string &what) {
	CappedString out, err;
	if (fn(out.Receive(), err.Receive()) != CAPPED_OK) {
		throw IOException("capped: %s failed: %s", what, err.Str());
	}
	return out.Str();
}

} // namespace

vector<CappedSegmentInfo> CappedReadSegments(const string &manifest_json) {
	vector<CappedSegmentInfo> result;
	auto segments_json =
	    FfiStr([&](char **out, char **err) { return capped_manifest_list_segments(manifest_json.c_str(), out, err); },
	           "listing segments");
	auto *root = ParseJsonRoot(segments_json, "segment list");
	if (!duckdb_yyjson::yyjson_is_arr(root)) {
		throw IOException("capped: corrupt segment list in manifest");
	}
	duckdb_yyjson::yyjson_arr_iter iter;
	duckdb_yyjson::yyjson_arr_iter_init(root, &iter);
	while (auto *seg = duckdb_yyjson::yyjson_arr_iter_next(&iter)) {
		CappedSegmentInfo info;
		auto *id = duckdb_yyjson::yyjson_obj_get(seg, "id");
		auto *path = duckdb_yyjson::yyjson_obj_get(seg, "path");
		auto *rows = duckdb_yyjson::yyjson_obj_get(seg, "rows");
		auto *bytes = duckdb_yyjson::yyjson_obj_get(seg, "bytes");
		auto *min_ts = duckdb_yyjson::yyjson_obj_get(seg, "min_ts");
		auto *max_ts = duckdb_yyjson::yyjson_obj_get(seg, "max_ts");
		auto *sealed = duckdb_yyjson::yyjson_obj_get(seg, "sealed");
		if (!id || !path || !duckdb_yyjson::yyjson_is_str(path)) {
			throw IOException("capped: corrupt segment entry in manifest");
		}
		info.id = duckdb_yyjson::yyjson_get_sint(id);
		info.path = duckdb_yyjson::yyjson_get_str(path);
		info.rows = rows ? duckdb_yyjson::yyjson_get_sint(rows) : 0;
		info.bytes = bytes ? duckdb_yyjson::yyjson_get_sint(bytes) : 0;
		// null/absent bounds = the table has no event time (see SegmentMeta)
		info.has_ts = min_ts && max_ts && duckdb_yyjson::yyjson_is_int(min_ts) &&
		              duckdb_yyjson::yyjson_is_int(max_ts);
		info.min_ts = info.has_ts ? duckdb_yyjson::yyjson_get_sint(min_ts) : 0;
		info.max_ts = info.has_ts ? duckdb_yyjson::yyjson_get_sint(max_ts) : 0;
		info.sealed = sealed && duckdb_yyjson::yyjson_is_true(sealed);
		// an absent/null index_file means the segment is not searchable: either the
		// table has no search columns, or its build failed (see capped_index)
		auto *index_file = duckdb_yyjson::yyjson_obj_get(seg, "index_file");
		info.has_index = index_file && duckdb_yyjson::yyjson_is_str(index_file);
		info.index_file = info.has_index ? duckdb_yyjson::yyjson_get_str(index_file) : string();
		result.push_back(std::move(info));
	}
	return result;
}

string CappedReadPolicy(const string &manifest_json) {
	return FfiStr(
	    [&](char **out, char **err) { return capped_manifest_policy_json(manifest_json.c_str(), out, err); },
	    "reading policy");
}

CappedSearchSpec CappedReadSearchSpec(const string &manifest_json) {
	CappedSearchSpec spec;
	auto search_json = FfiStr(
	    [&](char **out, char **err) { return capped_manifest_search_json(manifest_json.c_str(), out, err); },
	    "reading search configuration");
	auto *root = ParseJsonRoot(search_json, "search configuration");
	auto read_names = [&](const char *key, vector<string> &into) {
		auto *arr = duckdb_yyjson::yyjson_obj_get(root, key);
		if (!arr || !duckdb_yyjson::yyjson_is_arr(arr)) {
			return;
		}
		duckdb_yyjson::yyjson_arr_iter iter;
		duckdb_yyjson::yyjson_arr_iter_init(arr, &iter);
		while (auto *item = duckdb_yyjson::yyjson_arr_iter_next(&iter)) {
			if (duckdb_yyjson::yyjson_is_str(item)) {
				into.push_back(duckdb_yyjson::yyjson_get_str(item));
			}
		}
	};
	read_names("columns", spec.columns);
	read_names("vector_columns", spec.vector_columns);
	auto *metric = duckdb_yyjson::yyjson_obj_get(root, "metric");
	if (metric && duckdb_yyjson::yyjson_is_str(metric)) {
		spec.metric = duckdb_yyjson::yyjson_get_str(metric);
	}
	return spec;
}

CappedPolicySummary CappedReadPolicySummary(const string &manifest_json) {
	CappedPolicySummary out;
	auto policy_json = CappedReadPolicy(manifest_json);
	auto *doc = duckdb_yyjson::yyjson_read(policy_json.c_str(), policy_json.size(), 0);
	if (!doc) {
		throw IOException("capped: cannot parse policy summary");
	}
	auto *root = duckdb_yyjson::yyjson_doc_get_root(doc);
	auto read_str = [&](const char *key, string &field) {
		auto *v = duckdb_yyjson::yyjson_obj_get(root, key);
		if (v && duckdb_yyjson::yyjson_is_str(v)) {
			field = duckdb_yyjson::yyjson_get_str(v);
		}
	};
	auto read_int = [&](const char *key, bool &has, int64_t &field) {
		auto *v = duckdb_yyjson::yyjson_obj_get(root, key);
		// is_int, not is_sint: yyjson stores positive integers with the *uint*
		// subtype, and is_sint() would silently skip every cap we emit
		if (v && duckdb_yyjson::yyjson_is_int(v)) {
			has = true;
			field = duckdb_yyjson::yyjson_get_sint(v);
		}
	};
	read_str("time_col", out.time_col);
	read_int("ttl", out.has_ttl, out.ttl);
	read_int("max_bytes", out.has_max_bytes, out.max_bytes);
	read_int("max_rows", out.has_max_rows, out.max_rows);
	read_str("evict_order", out.evict_order);
	read_str("on_violation", out.on_violation);
	duckdb_yyjson::yyjson_doc_free(doc);
	return out;
}

void CappedFillScalars(QueryResult &result, vector<Value> &out, const string &what) {
	auto chunk = result.Fetch();
	if (!chunk || chunk->size() != 1) {
		throw IOException("capped: internal query for %s returned an unexpected number of rows", what);
	}
	out.clear();
	for (idx_t i = 0; i < result.types.size(); i++) {
		out.push_back(chunk->GetValue(i, 0));
	}
}

vector<string> CappedWrite::ParquetColumns(Connection &con, const string &abs_path) {
	auto q = con.Query("SELECT DISTINCT path_in_schema FROM parquet_metadata(" + CappedQuoteLit(abs_path) + ")");
	if (q->HasError()) {
		throw IOException("capped: reading parquet schema failed: %s", q->GetError());
	}
	vector<string> out;
	while (auto chunk = q->Fetch()) {
		for (idx_t r = 0; r < chunk->size(); r++) {
			out.push_back(chunk->GetValue(0, r).ToString());
		}
	}
	return out;
}

void CappedWrite::ComputeSegmentStats(Connection &con, const string &abs_path, const string &time_col,
                                      int64_t &rows, int64_t &bytes, bool &has_ts, int64_t &min_ts,
                                      int64_t &max_ts) {
	auto path_lit = CappedQuoteLit(abs_path);

	auto file_meta = con.Query("SELECT coalesce(num_rows, 0) FROM parquet_file_metadata(" + path_lit + ")");
	if (file_meta->HasError()) {
		throw IOException("capped: reading parquet metadata failed: %s", file_meta->GetError());
	}
	vector<Value> vals;
	CappedFillScalars(*file_meta, vals, "parquet row count");
	rows = vals[0].GetValue<int64_t>();

	auto sizes = con.Query("SELECT coalesce(sum(total_compressed_size), 0) FROM parquet_metadata(" + path_lit + ")");
	if (sizes->HasError()) {
		throw IOException("capped: reading parquet metadata failed: %s", sizes->GetError());
	}
	CappedFillScalars(*sizes, vals, "parquet byte sizes");
	bytes = vals[0].GetValue<int64_t>();

	min_ts = max_ts = 0;
	has_ts = false;
	if (time_col.empty() || rows == 0) {
		// no event-time source (or nothing written): report "unknown", never 0 —
		// a bogus 1970 bound would make age-based retention delete the segment
		return;
	}

	// a bulk-adopted file of an ingest-time table may legitimately lack the
	// store's hidden column: those rows have no known arrival time, so they are
	// never age-droppable. A *user* time column missing from the file is a real
	// policy/data mismatch and must not pass silently.
	auto file_cols = CappedWrite::ParquetColumns(con, abs_path);
	case_insensitive_set_t have(file_cols.begin(), file_cols.end());
	if (!have.count(time_col)) {
		if (time_col == CAPPED_INGEST_TS_COLUMN) {
			return;
		}
		throw IOException("capped: time column '%s' is missing from '%s' — retention by age cannot work", time_col,
		                  abs_path);
	}

	auto col = CappedQuoteIdent(time_col);
	auto ts = con.Query("SELECT count(*) FILTER (WHERE " + col + " IS NOT NULL), "
	                    "count(*) FILTER (WHERE TRY_CAST(" + col +
	                    " AS TIMESTAMPTZ) IS NOT NULL), "
	                    "coalesce(min(epoch_ms(TRY_CAST(" + col + " AS TIMESTAMPTZ))), 0) * 1000, "
	                    "coalesce(max(epoch_ms(TRY_CAST(" + col + " AS TIMESTAMPTZ))), 0) * 1000 "
	                    "FROM read_parquet(" +
	                    path_lit + ")");
	if (ts->HasError()) {
		throw IOException("capped: reading time column '%s' failed: %s", time_col, ts->GetError());
	}
	CappedFillScalars(*ts, vals, "event-time range");
	auto present = vals[0].GetValue<int64_t>();
	auto castable = vals[1].GetValue<int64_t>();
	if (present > 0 && castable == 0) {
		throw IOException("capped: time column '%s' holds no readable timestamps — retention by age "
		                  "cannot work (drop the time_col option, or fix the column's type)",
		                  time_col);
	}
	if (castable == 0) {
		return; // all-NULL column => no known range
	}
	has_ts = true;
	min_ts = vals[2].GetValue<int64_t>();
	max_ts = vals[3].GetValue<int64_t>();
}

namespace {

void CheckQuery(unique_ptr<QueryResult> result, const string &what) {
	if (result->HasError()) {
		throw IOException("capped: %s failed: %s", what, result->GetError());
	}
}

//! Deletes the merged temp file if publication fails before the move (a failed
//! stats computation or a rejected hard-cap commit must not leave garbage in
//! the table directory).
struct TempFileGuard {
	TempFileGuard(FileSystem &fs_p, string path_p) : fs(fs_p), path(std::move(path_p)) {
	}
	~TempFileGuard() {
		if (done) {
			return;
		}
		try {
			if (fs.FileExists(path)) {
				fs.RemoveFile(path);
			}
		} catch (...) { // best effort: nothing to do about a failed cleanup
		}
	}
	void Commit() {
		done = true;
	}

	FileSystem &fs;
	string path;
	bool done = false;
};

string PutSegment(const string &manifest_json, const string &segment_json) {
	return FfiStr(
	    [&](char **out, char **err) {
		    return capped_manifest_put_segment(manifest_json.c_str(), segment_json.c_str(), out, err);
	    },
	    "registering segment");
}

string SegmentJson(int64_t id, const string &rel_path, int64_t rows, int64_t bytes, bool has_ts, int64_t min_ts,
                   int64_t max_ts, bool sealed, int64_t sealed_at) {
	// unknown bounds are written as null, not 0: a fake 1970 bound would make
	// the segment look ancient and age-based retention would delete it
	auto bound = [&](bool valid, int64_t v) { return valid ? to_string(v) : string("null"); };
	return "{\"id\":" + to_string(id) + ",\"path\":\"" + CappedEscapeJson(rel_path) +
	       "\",\"rows\":" + to_string(rows) + ",\"bytes\":" + to_string(bytes) + ",\"min_ts\":" + bound(has_ts, min_ts) +
	       ",\"max_ts\":" + bound(has_ts, max_ts) + ",\"sealed\":" + (sealed ? "true" : "false") + ",\"sealed_at\":" +
	       (sealed ? to_string(sealed_at) : "null") + "}";
}

struct EvictionPlan {
	vector<int64_t> drop_ids;
	vector<string> drop_paths; // resolved by the caller via the segments snapshot
	bool still_over_budget = false;
};

//! Run the eviction planner; caller resolves drop ids against a segments
//! snapshot to delete the files after the manifest lands.
EvictionPlan PlanEviction(const string &manifest_json, int64_t now_micros) {
	EvictionPlan plan;
	auto plan_json = FfiStr([&](char **out, char **err) {
		return capped_plan_eviction(manifest_json.c_str(), now_micros, out, err);
	}, "planning eviction");
	auto *root = ParseJsonRoot(plan_json, "eviction plan");
	auto *drops = duckdb_yyjson::yyjson_obj_get(root, "drops");
	if (drops && duckdb_yyjson::yyjson_is_arr(drops)) {
		duckdb_yyjson::yyjson_arr_iter iter;
		duckdb_yyjson::yyjson_arr_iter_init(drops, &iter);
		while (auto *drop = duckdb_yyjson::yyjson_arr_iter_next(&iter)) {
			auto *id = duckdb_yyjson::yyjson_obj_get(drop, "id");
			if (id) {
				plan.drop_ids.push_back(duckdb_yyjson::yyjson_get_sint(id));
			}
		}
	}
	auto *over = duckdb_yyjson::yyjson_obj_get(root, "still_over_budget");
	plan.still_over_budget = over && duckdb_yyjson::yyjson_is_true(over);
	return plan;
}

string RemoveSegmentsFfi(const string &manifest_json, const vector<int64_t> &ids) {
	string ids_json = "[";
	for (idx_t i = 0; i < ids.size(); i++) {
		if (i > 0) {
			ids_json += ",";
		}
		ids_json += to_string(ids[i]);
	}
	ids_json += "]";
	return FfiStr([&](char **out, char **err) {
		return capped_manifest_remove_segments(manifest_json.c_str(), ids_json.c_str(), out, err);
	}, "removing segments");
}

//! {"on_violation":"evict", ...} -> "block"?
bool PolicyIsBlock(const string &policy_json) {
	auto *root = ParseJsonRoot(policy_json, "policy summary");
	auto *ov = duckdb_yyjson::yyjson_obj_get(root, "on_violation");
	return ov && duckdb_yyjson::yyjson_is_str(ov) && string(duckdb_yyjson::yyjson_get_str(ov)) == "block";
}

//! Evict sealed segments that breach the caps, then (optionally) seal the open
//! segment when it is large enough. Assumes the catalog write lock is held.
//! Returns after manifest + file deletions land; throws on hard-cap violation
//! under block (before any deletion, when check_block_pre_publish is true and
//! the table would stay over budget even after eviction).
CappedEnforceSummary EnforceUnlocked(CappedCatalog &catalog, const string &table, bool seal_open) {
	CappedEnforceSummary summary;
	auto &fs = catalog.FS();
	auto &storage = catalog.Storage();
	auto manifest = storage.ReadManifest(fs, table);
	auto now = CappedNowMicros();
	auto segments = CappedReadSegments(manifest);

	auto plan = PlanEviction(manifest, now);
	if (plan.still_over_budget && PolicyIsBlock(CappedReadPolicy(manifest))) {
		throw IOException("capped: table '%s' is over its hard cap and eviction cannot free enough "
		                  "(on_violation => block)",
		                  table);
	}
	if (!plan.drop_ids.empty()) {
		manifest = RemoveSegmentsFfi(manifest, plan.drop_ids);
		storage.WriteManifest(fs, table, manifest);
		for (auto &id : plan.drop_ids) {
			for (auto &seg : segments) {
				if (seg.id == id) {
					auto abs_path = storage.Root() + "/" + seg.path;
					if (fs.FileExists(abs_path)) {
						fs.RemoveFile(abs_path);
					}
					// the segment's search index goes with it: an index outliving
					// its data would leak disk and surface deleted rows in results
					CappedIndex::Remove(catalog, seg.index_file);
					break;
				}
			}
		}
		summary.dropped = plan.drop_ids.size();
	}
	if (seal_open) {
		int32_t seal = 0;
		CappedString err;
		if (capped_should_seal_default(manifest.c_str(), &seal, err.Receive()) != CAPPED_OK) {
			throw IOException("capped: seal check failed: %s", err.Str());
		}
		if (seal == 1) {
			// the open segment is by definition the only unsealed one
			const CappedSegmentInfo *open = nullptr;
			for (auto &seg : segments) {
				if (!seg.sealed) {
					open = &seg;
				}
			}
			if (open) {
				manifest = PutSegment(manifest, SegmentJson(open->id, open->path, open->rows, open->bytes,
				                                             open->has_ts, open->min_ts, open->max_ts, true, now));
				storage.WriteManifest(fs, table, manifest);
				summary.sealed = true;
			}
		}
	}
	return summary;
}

} // namespace

void CappedWrite::FlushBuffer(ClientContext &context, CappedCatalog &catalog, Connection &con, CappedBuffer &buffer) {
	auto &fs = catalog.FS();
	auto &storage = catalog.Storage();
	auto lock = catalog.LockWrites();

	if (buffer.appender) {
		buffer.appender->Close();
		buffer.appender.reset();
	}
	if (buffer.rows == 0) {
		return; // nothing written in this transaction for this table
	}

	auto manifest = storage.ReadManifest(fs, buffer.table);
	CappedString err;
	CappedString time_col;
	if (capped_manifest_time_col(manifest.c_str(), time_col.Receive(), err.Receive()) != CAPPED_OK) {
		throw IOException("capped: reading time column failed: %s", err.Str());
	}
	auto segments = CappedReadSegments(manifest);
	const CappedSegmentInfo *open = nullptr;
	for (auto &seg : segments) {
		if (!seg.sealed) {
			open = &seg;
		}
	}

	auto buf_name = CappedQuoteIdent(buffer.BufferTableName());
	auto tmp_rel = buffer.table + "/.flush-" + to_string(CappedNowMicros()) + ".tmp.parquet";
	auto tmp_abs = storage.Root() + "/" + tmp_rel;
	if (fs.FileExists(tmp_abs)) {
		fs.RemoveFile(tmp_abs);
	}

	// ingest-time tables gain their age column here: the buffer carries only the
	// user's columns, and the rows are stamped as they are written (now() is the
	// transaction's start timestamp, so one commit lands as one arrival time)
	string fresh_rows = "SELECT * FROM " + buf_name;
	if (time_col.Str() == CAPPED_INGEST_TS_COLUMN) {
		fresh_rows = "SELECT *, now() AS \"" CAPPED_INGEST_TS_COLUMN "\" FROM " + buf_name;
	}

	// merge: previous open segment (if any) + freshly committed rows
	string copy_sql;
	if (open) {
		auto old_abs = storage.Root() + "/" + open->path;
		if (!fs.FileExists(old_abs)) {
			throw IOException("capped: open segment '%s' of table '%s' is missing", open->path, buffer.table);
		}
		copy_sql = "COPY (SELECT * FROM read_parquet(" + CappedQuoteLit(old_abs) + ") UNION ALL BY NAME " +
		           fresh_rows + ") TO " + CappedQuoteLit(tmp_abs) + " (FORMAT parquet)";
	} else {
		copy_sql = "COPY (" + fresh_rows + ") TO " + CappedQuoteLit(tmp_abs) + " (FORMAT parquet)";
	}
	CheckQuery(con.Query(copy_sql), "merging insert buffer");
	TempFileGuard tmp_guard(fs, tmp_abs);

	int64_t rows = 0, bytes = 0, min_ts = 0, max_ts = 0;
	bool has_ts = false;
	ComputeSegmentStats(con, tmp_abs, time_col.Str(), rows, bytes, has_ts, min_ts, max_ts);

	int64_t seg_id;
	string rel_path;
	if (open) {
		seg_id = open->id;
		rel_path = open->path;
	} else {
		if (capped_manifest_next_segment_id(manifest.c_str(), &seg_id, err.Receive()) != CAPPED_OK) {
			throw IOException("capped: allocating segment id failed: %s", err.Str());
		}
		rel_path = buffer.table + "/" + to_string(seg_id) + ".seg.parquet";
	}

	// candidate manifest: open segment replaced by the merged file (still
	// unsealed, so the eviction planner protects it from deletion)
	auto candidate = PutSegment(manifest, SegmentJson(seg_id, rel_path, rows, bytes, has_ts, min_ts, max_ts, false, 0));

	// hard caps gate publication: if eviction alone cannot bring the table
	// back within budget, block rejects the commit with nothing published
	// (the temp file goes with it)
	auto plan = PlanEviction(candidate, CappedNowMicros());
	if (plan.still_over_budget && PolicyIsBlock(CappedReadPolicy(candidate))) {
		throw IOException("capped: insert into '%s' would exceed its hard cap (on_violation => block)", buffer.table);
	}
	if (!plan.drop_ids.empty()) {
		candidate = RemoveSegmentsFfi(candidate, plan.drop_ids);
	}

	// publish: new file first (same path as the old open segment, or a fresh
	// id), then the manifest atomically, then the evicted files
	fs.MoveFile(tmp_abs, storage.Root() + "/" + rel_path);
	tmp_guard.Commit();

	int32_t seal = 0;
	if (capped_should_seal_default(candidate.c_str(), &seal, err.Receive()) != CAPPED_OK) {
		throw IOException("capped: seal check failed: %s", err.Str());
	}
	if (seal == 1) {
		candidate = PutSegment(candidate,
		                       SegmentJson(seg_id, rel_path, rows, bytes, has_ts, min_ts, max_ts, true,
		                                   CappedNowMicros()));
	}
	storage.WriteManifest(fs, buffer.table, candidate);

	for (auto &id : plan.drop_ids) {
		for (auto &seg : segments) {
			if (seg.id == id) {
				auto abs_path = storage.Root() + "/" + seg.path;
				if (fs.FileExists(abs_path)) {
					fs.RemoveFile(abs_path);
				}
				CappedIndex::Remove(catalog, seg.index_file);
				break;
			}
		}
	}

	// a segment that just sealed (here or above) is now searchable; best effort,
	// an index is never worth failing a commit over
	string index_error;
	CappedIndex::EnsureUnlocked(catalog, buffer.table, index_error, CappedIndexMode::BuildMissing);
}

CappedEnforceSummary CappedWrite::Enforce(CappedCatalog &catalog, const string &table) {
	auto lock = catalog.LockWrites();
	auto summary = EnforceUnlocked(catalog, table, true);
	// sealing (above) can leave a segment without an index, and a bulk-adopted
	// file never had one: BuildMissing keeps this cheap once everything is
	// indexed, and cheap-again-after-a-failure thanks to the degraded latch
	string index_error;
	CappedIndex::EnsureUnlocked(catalog, table, index_error, CappedIndexMode::BuildMissing);
	return summary;
}

bool CappedWrite::ForceSeal(CappedCatalog &catalog, const string &table) {
	auto &fs = catalog.FS();
	auto &storage = catalog.Storage();
	auto lock = catalog.LockWrites();

	auto manifest = storage.ReadManifest(fs, table);
	const CappedSegmentInfo *open = nullptr;
	auto segments = CappedReadSegments(manifest);
	for (auto &seg : segments) {
		if (!seg.sealed) {
			open = &seg;
		}
	}
	if (!open) {
		return false;
	}
	manifest = PutSegment(manifest, SegmentJson(open->id, open->path, open->rows, open->bytes, open->has_ts,
	                                            open->min_ts, open->max_ts, true, CappedNowMicros()));
	storage.WriteManifest(fs, table, manifest);
	// the freshly sealed segment is now evictable: rotation + caps apply to it
	EnforceUnlocked(catalog, table, false);
	// ... and searchable, if the table asked for indexes
	string index_error;
	CappedIndex::EnsureUnlocked(catalog, table, index_error, CappedIndexMode::BuildMissing);
	return true;
}

// --- CappedInsert operator ---------------------------------------------------

namespace {

class CappedInsertSinkState : public GlobalSinkState {
public:
	atomic<idx_t> count {0};
};

class CappedInsertSourceState : public GlobalSourceState {
public:
	bool emitted = false;
};

} // namespace

CappedInsert::CappedInsert(PhysicalPlan &physical_plan, CappedTableEntry &entry_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::INSERT, {LogicalType::BIGINT}, estimated_cardinality),
      entry(entry_p) {
}

unique_ptr<GlobalSinkState> CappedInsert::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<CappedInsertSinkState>();
}

SinkResultType CappedInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &sink = input.global_state.Cast<CappedInsertSinkState>();
	auto &txn = CappedTransaction::Get(context.client, entry.catalog);
	txn.BufferChunk(context.client, entry, chunk);
	sink.count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

unique_ptr<GlobalSourceState> CappedInsert::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<CappedInsertSourceState>();
}

SourceResultType CappedInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSourceInput &input) const {
	auto &sink = sink_state->Cast<CappedInsertSinkState>();
	auto &src = input.global_state.Cast<CappedInsertSourceState>();
	if (!src.emitted) {
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(sink.count.load())));
		src.emitted = true;
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
	return SourceResultType::FINISHED;
}

InsertionOrderPreservingMap<string> CappedInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table"] = entry.name;
	return result;
}

} // namespace duckdb
