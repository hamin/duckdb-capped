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
#include "duckdb/main/config.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/query_result.hpp"

#include "yyjson.hpp"

#include <cctype>
#include <chrono>
#include <sys/stat.h>
#include <unordered_set>

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
		auto *frags = duckdb_yyjson::yyjson_obj_get(seg, "fragments");
		info.fragments = frags && duckdb_yyjson::yyjson_is_int(frags) ? duckdb_yyjson::yyjson_get_sint(frags) : 0;
		auto *files = duckdb_yyjson::yyjson_obj_get(seg, "files");
		if (files && duckdb_yyjson::yyjson_is_arr(files)) {
			duckdb_yyjson::yyjson_arr_iter fit;
			duckdb_yyjson::yyjson_arr_iter_init(files, &fit);
			while (auto *file = duckdb_yyjson::yyjson_arr_iter_next(&fit)) {
				if (duckdb_yyjson::yyjson_is_str(file)) {
					info.files.push_back(duckdb_yyjson::yyjson_get_str(file));
				}
			}
		}
		// an absent/null index_file means the segment is not searchable: either the
		// table has no search columns, or its build failed (see capped_index)
		auto *index_file = duckdb_yyjson::yyjson_obj_get(seg, "index_file");
		info.has_index = index_file && duckdb_yyjson::yyjson_is_str(index_file);
		info.index_file = info.has_index ? duckdb_yyjson::yyjson_get_str(index_file) : string();
		result.push_back(std::move(info));
	}
	return result;
}

CappedManifestStats CappedReadManifestStats(const string &manifest_json) {
	CappedManifestStats stats;
	CappedString err;
	if (capped_manifest_stats(manifest_json.c_str(), &stats.bytes_written, &stats.pending_delete_files,
	                          err.Receive()) != CAPPED_OK) {
		throw IOException("capped: reading table stats failed: %s", err.Str());
	}
	return stats;
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

// --- fragment write path helpers (fragments plan §3.3–§3.5) ------------------

string AddFragmentFfi(const string &manifest_json, int64_t segment, const string &fragment_json) {
	return FfiStr([&](char **out, char **err) {
		return capped_manifest_add_fragment(manifest_json.c_str(), segment, fragment_json.c_str(), out, err);
	}, "adding fragment");
}

int64_t NextFragmentSeq(const string &manifest_json) {
	CappedString err;
	int64_t seq = -1;
	if (capped_manifest_next_fragment_seq(manifest_json.c_str(), &seq, err.Receive()) != CAPPED_OK) {
		throw IOException("capped: reading next fragment seq failed: %s", err.Str());
	}
	return seq;
}

string ApplyCompactionFfi(const string &manifest_json, const string &plan_json, const string &output_fragment_json,
                          int64_t now_micros) {
	return FfiStr([&](char **out, char **err) {
		return capped_manifest_apply_compaction(manifest_json.c_str(), plan_json.c_str(),
		                                        output_fragment_json.c_str(), now_micros, out, err);
	}, "applying fragment compaction");
}

//! A FragmentMeta JSON for the FFI (unknown bounds as null, never 0).
string FragmentJson(int64_t seq, const string &rel_path, int64_t rows, int64_t bytes, bool has_ts, int64_t min_ts,
                    int64_t max_ts, int64_t tier) {
	auto bound = [&](bool valid, int64_t v) { return valid ? to_string(v) : string("null"); };
	return "{\"seq\":" + to_string(seq) + ",\"path\":\"" + CappedEscapeJson(rel_path) +
	       "\",\"rows\":" + to_string(rows) + ",\"bytes\":" + to_string(bytes) + ",\"min_ts\":" + bound(has_ts, min_ts) +
	       ",\"max_ts\":" + bound(has_ts, max_ts) + ",\"tier\":" + to_string(tier) + "}";
}

//! Fragment file naming (plan §3.1): <segment_id>.frag-<seq zero-padded to 6>.
string FragmentRelPath(const string &table, int64_t segment, int64_t seq) {
	auto digits = to_string(seq);
	if (digits.size() < 6) {
		digits = string(6 - digits.size(), '0') + digits;
	}
	return table + "/" + to_string(segment) + ".frag-" + digits + ".parquet";
}

//! A merge plan from capped_plan_compaction (input_paths are the input seqs'
//! files, in order — added by the ABI for exactly this COPY).
struct FragmentMergePlan {
	int64_t segment = 0;
	int64_t output_tier = 0;
	vector<string> input_paths;
};

FragmentMergePlan ParseMergePlan(const string &plan_json) {
	FragmentMergePlan plan;
	auto *root = ParseJsonRoot(plan_json, "compaction plan");
	auto *segment = duckdb_yyjson::yyjson_obj_get(root, "segment");
	auto *tier = duckdb_yyjson::yyjson_obj_get(root, "output_tier");
	auto *inputs = duckdb_yyjson::yyjson_obj_get(root, "inputs");
	auto *paths = duckdb_yyjson::yyjson_obj_get(root, "input_paths");
	if (!segment || !tier || !inputs || !paths || !duckdb_yyjson::yyjson_is_arr(inputs) ||
	    !duckdb_yyjson::yyjson_is_arr(paths)) {
		throw IOException("capped: corrupt compaction plan");
	}
	plan.segment = duckdb_yyjson::yyjson_get_sint(segment);
	plan.output_tier = duckdb_yyjson::yyjson_get_sint(tier);
	duckdb_yyjson::yyjson_arr_iter iter;
	duckdb_yyjson::yyjson_arr_iter_init(paths, &iter);
	while (auto *p = duckdb_yyjson::yyjson_arr_iter_next(&iter)) {
		if (duckdb_yyjson::yyjson_is_str(p)) {
			plan.input_paths.push_back(duckdb_yyjson::yyjson_get_str(p));
		}
	}
	if (plan.input_paths.size() != duckdb_yyjson::yyjson_arr_size(inputs) || plan.input_paths.empty()) {
		throw IOException("capped: compaction plan input_paths do not match its inputs");
	}
	return plan;
}

//! Does the manifest reference `path` (segment data, fragments, index files,
//! pending deletes)? Used by the seal to decide whether the planned sealed
//! path is occupied (plan §3.5 step 2).
bool ManifestOwnsPath(const string &manifest_json, const string &path) {
	auto paths_json = FfiStr(
	    [&](char **out, char **err) { return capped_manifest_owned_paths(manifest_json.c_str(), out, err); },
	    "listing owned paths");
	auto *root = ParseJsonRoot(paths_json, "owned path list");
	if (!duckdb_yyjson::yyjson_is_arr(root)) {
		throw IOException("capped: corrupt owned path list");
	}
	duckdb_yyjson::yyjson_arr_iter iter;
	duckdb_yyjson::yyjson_arr_iter_init(root, &iter);
	while (auto *p = duckdb_yyjson::yyjson_arr_iter_next(&iter)) {
		if (duckdb_yyjson::yyjson_is_str(p) && path == duckdb_yyjson::yyjson_get_str(p)) {
			return true;
		}
	}
	return false;
}

//! Is this path currently occupied on disk? FileExists reports regular files
//! only, but a planted DIRECTORY blocks a name just as hard — MoveFile onto
//! one fails — so both checks are needed, the same idiom DuckDB's own
//! file_system uses. A blocked name must never be chosen: the manifest
//! consumes a fragment seq (or plans a sealed path) only when it lands, so
//! a collision would be retried, and fail, on every later commit. Skipped
//! leftovers are unreferenced pattern matches; the ATTACH GC collects them
//! later (§3.7).
bool PathOccupied(CappedCatalog &catalog, const string &abs_path) {
	auto &fs = catalog.FS();
	return fs.FileExists(abs_path) || fs.DirectoryExists(abs_path);
}

//! Next fragment seq at or after start_seq whose final path is free on disk
//! (§3.3/§3.4). add_fragment/apply_compaction accept any seq >= the
//! segment's next_fragment_seq, so skipped seqs are simply burned — and
//! never reused (§3.2). Only the candidate path is checked, never the whole
//! directory: one stat per probe, not one listing per candidate.
int64_t NextFreeFragmentSeq(CappedCatalog &catalog, const string &table, int64_t segment, int64_t start_seq) {
	auto &root = catalog.Storage().Root();
	for (auto seq = start_seq;; seq++) {
		if (!PathOccupied(catalog, root + "/" + FragmentRelPath(table, segment, seq))) {
			return seq;
		}
	}
}

//! Planned sealed path for a fresh open segment (§3.1): the reserved
//! <id>.seg.parquet, or the reserved unique <id>.seg-<micros>.parquet when
//! that name is taken on disk or by the manifest. A file adopted through
//! capped_add_segment can own a name a much later open segment would pick,
//! and segment ids are never reused — without this a wedge there would fail
//! every later INSERT (T9). Both forms match the orphan-GC patterns, so a
//! crash leftover here stays collectable (§3.7).
string PlannedSealRelPath(CappedCatalog &catalog, const string &table, int64_t segment,
                          const string &manifest_json) {
	auto &root = catalog.Storage().Root();
	string rel = table + "/" + to_string(segment) + ".seg.parquet";
	if (!PathOccupied(catalog, root + "/" + rel) && !ManifestOwnsPath(manifest_json, rel)) {
		return rel;
	}
	for (auto stamp = CappedNowMicros();; stamp++) {
		rel = table + "/" + to_string(segment) + ".seg-" + to_string(stamp) + ".parquet";
		if (!PathOccupied(catalog, root + "/" + rel) && !ManifestOwnsPath(manifest_json, rel)) {
			return rel;
		}
	}
}


string SealLegacyOpenFfi(const string &manifest_json, int64_t now_micros) {
	return FfiStr([&](char **out, char **err) {
		return capped_manifest_seal_legacy_open(manifest_json.c_str(), now_micros, out, err);
	}, "migrating legacy open segment");
}

string SealOpenFfi(const string &manifest_json, const string &output_segment_json, int64_t now_micros) {
	return FfiStr([&](char **out, char **err) {
		return capped_manifest_seal_open(manifest_json.c_str(), output_segment_json.c_str(), now_micros, out, err);
	}, "sealing open segment");
}

//! Pending-delete grace from the Rust core (fragments plan §3.2): a queued
//! delete becomes sweepable DELETE_GRACE after it was queued, and orphan GC
//! never collects a candidate younger than it.
int64_t CappedDeleteGraceMicros() {
	int64_t grace = 5 * 60 * 1000000;
	CappedString err;
	if (capped_delete_grace_micros(&grace, err.Receive()) != CAPPED_OK) {
		throw IOException("capped: reading the delete grace failed: %s", err.Str());
	}
	return grace;
}

//! The grace orphan GC enforces on candidate age, in micros: the
//! capped_pending_delete_grace setting when set (a VARCHAR duration: "5 min",
//! "30s", "250ms", bare number = micros, "0" = collect immediately), else the
//! Rust DELETE_GRACE. Test-only knob: it changes the age floor GC applies and
//! the grace NEWLY queued deletes get — not_before values already written
//! into a manifest are honoured exactly as stored.
//! Grammar of the capped_pending_delete_grace setting (fragments plan §3.7
//! test knob): a number with a unit suffix — us/micro(s|second)(s),
//! ms/milli(s|second)(s), s/sec(ond)(s), m/min(ute)(s), h/hr(s)/hour(s),
//! d/day(s), case-insensitive, optional spaces — or a bare number of
//! microseconds; "0" means "collect immediately". False on anything else.
//! Shared by the SET-time validation (capped_extension.cpp) and the reader.
bool ParseGraceText(const string &text, int64_t &out_micros) {
	auto lower = StringUtil::Lower(text);
	StringUtil::Trim(lower);
	size_t digits = 0;
	while (digits < lower.size() && (isdigit((unsigned char)lower[digits]) || lower[digits] == '.')) {
		digits += 1;
	}
	if (digits == 0) {
		return false;
	}
	auto unit = lower.substr(digits);
	while (!unit.empty() && isspace((unsigned char)unit[0])) {
		unit = unit.substr(1);
	}
	int64_t mult; // microseconds per unit; empty unit = micros
	if (unit.empty() || unit == "us" || unit == "micros" || unit == "microsecond" ||
	    unit == "microseconds") {
		mult = 1;
	} else if (unit == "ms" || unit == "millis" || unit == "millisecond" || unit == "milliseconds") {
		mult = 1000;
	} else if (unit == "s" || unit == "sec" || unit == "secs" || unit == "second" || unit == "seconds") {
		mult = 1000000;
	} else if (unit == "m" || unit == "min" || unit == "mins" || unit == "minute" || unit == "minutes") {
		mult = 60LL * 1000000;
	} else if (unit == "h" || unit == "hr" || unit == "hrs" || unit == "hour" || unit == "hours") {
		mult = 3600LL * 1000000;
	} else if (unit == "d" || unit == "day" || unit == "days") {
		mult = 86400LL * 1000000;
	} else {
		return false;
	}
	try {
		double value = std::stod(lower.substr(0, digits));
		if (value < 0 || value > 1e12 / (double)mult) {
			return false;
		}
		out_micros = (int64_t)(value * (double)mult);
		return true;
	} catch (...) {
		return false;
	}
}

int64_t PendingDeleteGrace(CappedCatalog &catalog) {
	auto &config = DBConfig::Get(catalog.GetAttached());
	ExtensionOption option;
	if (config.TryGetExtensionOption("capped_pending_delete_grace", option) && option.setting_index.IsValid()) {
		// the SET value lives in the user-settings store under the setting
		// index; absent means "not set" -> the built-in grace. SET validated
		// the text already; the fallback here only guards against values that
		// entered without the SET callback (config files), where behaving
		// like the default beats failing unrelated queries.
		Value set_value;
		if (config.user_settings.TryGetSetting(option.setting_index.GetIndex(), set_value) &&
		    !set_value.IsNull() && set_value.type() == LogicalType::VARCHAR) {
			int64_t micros = 0;
			if (ParseGraceText(set_value.GetValue<string>(), micros)) {
				return micros;
			}
		}
	}
	return CappedDeleteGraceMicros();
}

//! Shift added to the `now` handed to the delete-queueing FFI calls so new
//! pending deletes land at not_before = real_now + grace (Rust schedules
//! not_before = now + DELETE_GRACE internally). Zero under the default grace;
//! the test-only override moves it, dragging sealed_at along — acceptable
//! for a debug knob, never reachable without an explicit SET.
int64_t GraceQueueOffset(CappedCatalog &catalog) {
	return PendingDeleteGrace(catalog) - CappedDeleteGraceMicros();
}

//! Strings from a JSON array of strings (owned paths, sweep output).
vector<string> ParseStringArray(const string &json, const string &what) {
	auto *root = ParseJsonRoot(json, what);
	if (!duckdb_yyjson::yyjson_is_arr(root)) {
		throw IOException("capped: corrupt %s", what);
	}
	vector<string> out;
	duckdb_yyjson::yyjson_arr_iter iter;
	duckdb_yyjson::yyjson_arr_iter_init(root, &iter);
	while (auto *item = duckdb_yyjson::yyjson_arr_iter_next(&iter)) {
		if (!duckdb_yyjson::yyjson_is_str(item)) {
			throw IOException("capped: corrupt %s", what);
		}
		out.emplace_back(duckdb_yyjson::yyjson_get_str(item));
	}
	return out;
}

//! Does a top-level file name match one of the four capped-owned leftover
//! patterns orphan GC may collect (fragments plan §3.7)? Everything else —
//! user parquet, other extensions, odd shapes — is untouchable, and
//! "<id>.seg.parquet" (a planned path, §4 crash table) counts only for the
//! CURRENT open segment: a sealed segment's <id>.seg.parquet is live data.
bool MatchesOrphanGcPattern(const string &name, const string &open_segment_id) {
	auto all_digits = [](const string &s) {
		return !s.empty() && s.find_first_not_of("0123456789") == string::npos;
	};
	// .flush-<digits>.tmp.parquet: fragment, merge and seal temporaries
	const string tmp_prefix = ".flush-", tmp_suffix = ".tmp.parquet";
	if (name.rfind(tmp_prefix, 0) == 0 && name.size() > tmp_prefix.size() + tmp_suffix.size() &&
	    name.compare(name.size() - tmp_suffix.size(), tmp_suffix.size(), tmp_suffix) == 0) {
		return all_digits(name.substr(tmp_prefix.size(), name.size() - tmp_prefix.size() - tmp_suffix.size()));
	}
	auto dot = name.find('.');
	if (dot == string::npos || dot == 0) {
		return false;
	}
	auto id_part = name.substr(0, dot);
	if (!all_digits(id_part)) {
		return false;
	}
	auto rest = name.substr(dot + 1);
	const string parquet_suffix = ".parquet";
	// <id>.frag-<digits>.parquet
	if (rest.rfind("frag-", 0) == 0 && rest.size() > 5 + parquet_suffix.size() &&
	    rest.compare(rest.size() - parquet_suffix.size(), parquet_suffix.size(), parquet_suffix) == 0 &&
	    all_digits(rest.substr(5, rest.size() - 5 - parquet_suffix.size()))) {
		return true;
	}
	// <id>.seg-<digits>.parquet: the seal fallback name (§3.5)
	if (rest.rfind("seg-", 0) == 0 && rest.size() > 4 + parquet_suffix.size() &&
	    rest.compare(rest.size() - parquet_suffix.size(), parquet_suffix.size(), parquet_suffix) == 0 &&
	    all_digits(rest.substr(4, rest.size() - 4 - parquet_suffix.size()))) {
		return true;
	}
	// <id>.seg.parquet: only the open segment's planned path
	return !open_segment_id.empty() && name == open_segment_id + ".seg.parquet";
}

//! Last-modified time of a regular file in micros; -1 if it cannot be read
//! (gone, stat failed), -2 for anything that is not a regular file — GC must
//! never RemoveFile() a directory or a socket it mistook for leftover.
int64_t FileModifiedMicros(const string &abs_path) {
	struct stat st;
	if (stat(abs_path.c_str(), &st) != 0) {
		return -1;
	}
	if (!S_ISREG(st.st_mode)) {
		return -2;
	}
#if defined(__APPLE__)
	return (int64_t)st.st_mtime * 1000000 + (int64_t)st.st_mtimespec.tv_nsec / 1000;
#else
	return (int64_t)st.st_mtime * 1000000 + (int64_t)st.st_mtim.tv_nsec / 1000;
#endif
}

//! Pending-delete sweep (fragments plan §3.7). The manifest is the commit
//! point: due paths leave pending_deletes in one atomic swap FIRST, and only
//! then are the files removed — a crash in between leaves unreferenced files
//! on disk that the ATTACH-time orphan GC collects later. A file already gone
//! is fine; any other delete error is recorded, never rethrown (forgetting
//! the path in the manifest is what matters; the caller cannot act on this).
void SweepUnlocked(CappedCatalog &catalog, const string &table) {
	if (catalog.ReadOnly()) {
		return; // a READ_ONLY attach deletes nothing
	}
	auto &fs = catalog.FS();
	auto &storage = catalog.Storage();
	auto manifest = storage.ReadManifest(fs, table);
	vector<string> paths;
	CappedString swept_manifest, swept_paths, sweep_err;
	if (capped_manifest_sweep_due(manifest.c_str(), CappedNowMicros(), swept_manifest.Receive(),
	                              swept_paths.Receive(), sweep_err.Receive()) != CAPPED_OK) {
		throw IOException("capped: sweeping due pending deletes failed: %s", sweep_err.Str());
	}
	for (auto &rel : ParseStringArray(swept_paths.Str(), "pending-delete sweep output")) {
		paths.push_back(rel);
	}
	if (paths.empty()) {
		return; // nothing due: the manifest stands untouched
	}
	storage.WriteManifest(fs, table, swept_manifest.Str()); // manifest first: the commit point
	for (auto &rel : paths) {
		auto abs_path = storage.Root() + "/" + rel;
		try {
			if (fs.FileExists(abs_path)) {
				fs.RemoveFile(abs_path);
			}
		} catch (std::exception &ex) {
			catalog.RecordMaintenanceFailure(table, "sweep delete " + rel + ": " + string(ex.what()));
		}
	}
}

//! Orphan GC (fragments plan §3.7), ATTACH only — the most dangerous code
//! in the extension, so every rule here is a guard against deleting live
//! data: top level of the table directory only (no recursion); names matching
//! one capped-owned leftover pattern exactly; never anything the manifest
//! still owns (segments, fragments, index sidecars, pending deletes — via
//! capped_manifest_owned_paths); never a file younger than the grace (there
//! is no cross-process lock — a peer may be mid-write, and a fresh crash
//! leftover simply waits for a later ATTACH).
void OrphanGcUnlocked(CappedCatalog &catalog, const string &table, const string &manifest) {
	if (catalog.ReadOnly()) {
		return; // a READ_ONLY attach deletes nothing
	}
	auto &fs = catalog.FS();
	auto &storage = catalog.Storage();
	std::unordered_set<string> owned;
	for (auto &rel : ParseStringArray(
	         FfiStr([&](char **out, char **e) {
		         return capped_manifest_owned_paths(manifest.c_str(), out, e);
	         }, "listing owned paths"),
	         "owned path list")) {
		owned.insert(rel);
	}
	string open_id;
	for (auto &seg : CappedReadSegments(manifest)) {
		if (!seg.sealed) {
			open_id = to_string(seg.id);
			break;
		}
	}
	string dir = storage.Root() + "/" + table;
	vector<string> candidates;
	try {
		fs.ListFiles(dir, [&](const string &name, bool is_dir) {
			if (!is_dir && MatchesOrphanGcPattern(name, open_id)) {
				candidates.push_back(name);
			}
		});
	} catch (std::exception &ex) {
		catalog.RecordMaintenanceFailure(table, "orphan gc listing: " + string(ex.what()));
		return;
	}
	int64_t grace = PendingDeleteGrace(catalog);
	int64_t now = CappedNowMicros();
	for (auto &name : candidates) {
		auto rel = table + "/" + name;
		if (owned.count(rel) != 0) {
			continue; // the manifest still references it: not garbage
		}
		auto abs_path = dir + "/" + name;
		auto mtime = FileModifiedMicros(abs_path);
		if (mtime < 0) {
			continue; // vanished, or not a regular file
		}
		if (now - mtime < grace) {
			continue; // may belong to a peer mid-write; a later ATTACH collects it
		}
		try {
			if (fs.FileExists(abs_path)) {
				fs.RemoveFile(abs_path);
			}
		} catch (std::exception &ex) {
			catalog.RecordMaintenanceFailure(table, "orphan gc delete " + rel + ": " + string(ex.what()));
		}
	}
}

//! Seal = final compaction (plan §3.5), shared by FlushBuffer, EnforceUnlocked
//! and ForceSeal. The open segment's fragments are merged into one file at the
//! planned path — or adopted as-is when there is exactly one fragment (free
//! seal) — with stats recomputed from the output so sealed stats stay exact;
//! then the manifest swap commits. A crash before that swap leaves only an
//! unreferenced `<id>.seg*.parquet` (removed by the ATTACH GC, plan §4). A
//! legacy (pre-fragment) open segment is migrated in place instead (§3.8).
//! Assumes the catalog write lock is held. Returns false when there is nothing
//! to seal (no open segment, or an empty fresh one).
bool SealOpenUnlocked(CappedCatalog &catalog, const string &table) {
	auto &fs = catalog.FS();
	auto &storage = catalog.Storage();
	auto manifest = storage.ReadManifest(fs, table);
	auto segments = CappedReadSegments(manifest);
	const CappedSegmentInfo *open = nullptr;
	for (auto &seg : segments) {
		if (!seg.sealed) {
			open = &seg;
		}
	}
	if (!open) {
		return false;
	}
	if (open->fragments == 0) {
		if (open->rows == 0) {
			return false; // fresh open segment: nothing has been written yet
		}
		// legacy open segment (plan §3.8): complete Parquet file already at
		// its path, so migration only flips the seal flag
		manifest = SealLegacyOpenFfi(manifest, CappedNowMicros() + GraceQueueOffset(catalog));
		storage.WriteManifest(fs, table, manifest);
		return true;
	}

	auto now = CappedNowMicros();
	string output_rel;
	unique_ptr<TempFileGuard> output_guard; // protects only files this seal wrote
	if (open->fragments == 1 && open->files.size() == 1) {
		// single-fragment fast path: the fragment becomes the sealed file
		output_rel = open->files[0];
	} else {
		// output path: the planned one, or the reserved unique fallback when
		// that name is taken on disk (file or directory, see PathOccupied)
		// or by the manifest — never overwrite (§3.1)
		output_rel = open->path;
		if (PathOccupied(catalog, storage.Root() + "/" + output_rel) || ManifestOwnsPath(manifest, output_rel)) {
			auto stamp = now;
			do {
				output_rel = table + "/" + to_string(open->id) + ".seg-" + to_string(stamp) + ".parquet";
				stamp++;
			} while (PathOccupied(catalog, storage.Root() + "/" + output_rel));
		}
		// merge all fragments in insertion order, via a temp name so a crash
		// mid-COPY never leaves a half-written file at the sealed name
		string files_lit = "[";
		for (idx_t i = 0; i < open->files.size(); i++) {
			if (i > 0) {
				files_lit += ",";
			}
			files_lit += CappedQuoteLit(storage.Root() + "/" + open->files[i]);
		}
		files_lit += "]";
		auto tmp_rel = table + "/.flush-" + to_string(CappedNowMicros()) + ".tmp.parquet";
		auto tmp_abs = storage.Root() + "/" + tmp_rel;
		if (fs.FileExists(tmp_abs)) {
			fs.RemoveFile(tmp_abs);
		}
		Connection con(catalog.GetDatabase());
		CheckQuery(con.Query("COPY (SELECT * FROM read_parquet(" + files_lit + ")) TO " + CappedQuoteLit(tmp_abs) +
		                     " (FORMAT parquet)"),
		           "merging fragments for seal");
		TempFileGuard tmp_guard(fs, tmp_abs);
		fs.MoveFile(tmp_abs, storage.Root() + "/" + output_rel);
		tmp_guard.Commit();
		output_guard = make_uniq<TempFileGuard>(fs, storage.Root() + "/" + output_rel);
	}

	// exact stats from the output file itself (the manifest requires sealed
	// stats to be final; seal_open also rejects any row-count drift)
	CappedString time_col, err;
	if (capped_manifest_time_col(manifest.c_str(), time_col.Receive(), err.Receive()) != CAPPED_OK) {
		throw IOException("capped: reading time column failed: %s", err.Str());
	}
	int64_t rows = 0, bytes = 0, min_ts = 0, max_ts = 0;
	bool has_ts = false;
	{
		Connection con(catalog.GetDatabase());
		CappedWrite::ComputeSegmentStats(con, storage.Root() + "/" + output_rel, time_col.Str(), rows, bytes, has_ts,
		                                 min_ts, max_ts);
	}
	manifest = SealOpenFfi(manifest, SegmentJson(open->id, output_rel, rows, bytes, has_ts, min_ts, max_ts, true, now),
	                       now + GraceQueueOffset(catalog));
	storage.WriteManifest(fs, table, manifest); // the commit point
	if (output_guard) {
		output_guard->Commit();
	}
	return true;
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

	// pending-delete sweep (§3.7): files past their not_before go first,
	// before this pass can publish any further manifest
	SweepUnlocked(catalog, table);
	manifest = storage.ReadManifest(fs, table);

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
		if (seal == 1 && SealOpenUnlocked(catalog, table)) {
			summary.sealed = true;
		}
	}
	return summary;
}

} // namespace

void CappedWrite::FlushBuffer(ClientContext &context, CappedCatalog &catalog, Connection &con, CappedBuffer &buffer) {
	if (catalog.ReadOnly()) {
		throw IOException("capped: cannot insert into '%s': the store is attached READ_ONLY", buffer.table);
	}
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

	// ingest-time tables gain their age column here: the buffer carries only the
	// user's columns, and the rows are stamped as they are written (now() is the
	// transaction's start timestamp, so one commit lands as one arrival time)
	string fresh_rows = "SELECT * FROM " + buf_name;
	if (time_col.Str() == CAPPED_INGEST_TS_COLUMN) {
		fresh_rows = "SELECT *, now() AS \"" CAPPED_INGEST_TS_COLUMN "\" FROM " + buf_name;
	}

	// a legacy (pre-fragment) open segment cannot take fragments: migrate it
	// first (plan §3.8) and start this commit on a fresh fragment-based one
	if (open && open->fragments == 0 && open->rows > 0) {
		manifest = SealLegacyOpenFfi(manifest, CappedNowMicros() + GraceQueueOffset(catalog));
		storage.WriteManifest(fs, buffer.table, manifest);
		open = nullptr;
	}

	int64_t seg_id;
	if (open) {
		seg_id = open->id;
	} else if (capped_manifest_next_segment_id(manifest.c_str(), &seg_id, err.Receive()) != CAPPED_OK) {
		throw IOException("capped: allocating segment id failed: %s", err.Str());
	}
	auto seq = NextFreeFragmentSeq(catalog, buffer.table, seg_id, open ? NextFragmentSeq(manifest) : 0);

	// the commit's rows become one new immutable fragment: only the fresh
	// rows are written, existing data is never re-read (plan §3.3 step 1).
	// Written to a temp name and renamed, so a crash can never leave a
	// half-written file the manifest could name.
	auto frag_rel = FragmentRelPath(buffer.table, seg_id, seq);
	auto frag_abs = storage.Root() + "/" + frag_rel;
	auto tmp_rel = buffer.table + "/.flush-" + to_string(CappedNowMicros()) + ".tmp.parquet";
	auto tmp_abs = storage.Root() + "/" + tmp_rel;
	if (fs.FileExists(tmp_abs)) {
		fs.RemoveFile(tmp_abs);
	}
	CheckQuery(con.Query("COPY (" + fresh_rows + ") TO " + CappedQuoteLit(tmp_abs) + " (FORMAT parquet)"),
	           "writing insert fragment");
	TempFileGuard tmp_guard(fs, tmp_abs);

	int64_t rows = 0, bytes = 0, min_ts = 0, max_ts = 0;
	bool has_ts = false;
	ComputeSegmentStats(con, tmp_abs, time_col.Str(), rows, bytes, has_ts, min_ts, max_ts);

	// candidate manifest: the open segment (freshly planned when none exists,
	// aggregates kept in step with its fragments by the FFI) plus this fragment
	auto candidate = manifest;
	if (!open) {
		candidate = PutSegment(candidate,
		                       SegmentJson(seg_id, PlannedSealRelPath(catalog, buffer.table, seg_id, manifest),
		                                   0, 0, false, 0, 0, false, 0));
	}
	candidate = AddFragmentFfi(candidate, seg_id, FragmentJson(seq, frag_rel, rows, bytes, has_ts, min_ts, max_ts, 0));

	// hard caps gate publication: if eviction alone cannot bring the table
	// back within budget, block rejects the commit with nothing published
	// (the temp file goes with it, and so does the candidate)
	auto plan = PlanEviction(candidate, CappedNowMicros());
	if (plan.still_over_budget && PolicyIsBlock(CappedReadPolicy(candidate))) {
		throw IOException("capped: insert into '%s' would exceed its hard cap (on_violation => block)", buffer.table);
	}
	if (!plan.drop_ids.empty()) {
		candidate = RemoveSegmentsFfi(candidate, plan.drop_ids);
	}

	// publish: the fragment file lands under its final (still unreferenced)
	// name first, then the manifest rename — the single commit point (plan §4)
	fs.MoveFile(tmp_abs, frag_abs);
	tmp_guard.Commit();
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

	// Maintenance: minor compaction (plan §3.4) and the seal check (§3.3
	// step 8). The commit is already durable above, so a hiccup here must not
	// surface as a commit error — the rows are published and the next commit,
	// enforce pass, or background sweep retries the step. Each step keeps its
	// own write → manifest-swap commit point; unpublished outputs are deleted.
	auto record_maintenance_failure = [&](const char *step, const std::exception &ex) {
		catalog.RecordMaintenanceFailure(buffer.table, string(step) + ": " + string(ex.what()));
	};
	try {
		for (;;) {
			auto plan_json = FfiStr(
			    [&](char **out, char **e) { return capped_plan_compaction(candidate.c_str(), out, e); },
			    "planning fragment compaction");
			if (plan_json == "null") {
				break; // no run of K contiguous same-tier fragments left
			}
			auto merge = ParseMergePlan(plan_json);
			// The merge output wants the manifest's next seq; a foreign file or
			// directory sitting there must not kill the merge (it would retry,
			// and fail, forever — the manifest only consumes a seq when it
			// lands), so take the next free one. The blocked natural seq is
			// surfaced as a maintenance failure: a merge silently rerouting
			// around planted names would hide a real problem (§3.4, T9).
			auto want_seq = NextFragmentSeq(candidate);
			auto out_seq = want_seq;
			while (PathOccupied(catalog, storage.Root() + "/" +
			                           FragmentRelPath(buffer.table, merge.segment, out_seq))) {
				out_seq++;
			}
			if (out_seq != want_seq) {
				catalog.RecordMaintenanceFailure(buffer.table,
				                                 "fragment compaction: path " +
				                                     FragmentRelPath(buffer.table, merge.segment, want_seq) +
				                                     " is occupied on disk; wrote the merge output under seq " +
				                                     to_string(out_seq));
			}
			auto out_rel = FragmentRelPath(buffer.table, merge.segment, out_seq);
			auto out_abs = storage.Root() + "/" + out_rel;
			string files_lit = "[";
			for (idx_t i = 0; i < merge.input_paths.size(); i++) {
				if (i > 0) {
					files_lit += ",";
				}
				files_lit += CappedQuoteLit(storage.Root() + "/" + merge.input_paths[i]);
			}
			files_lit += "]";
			auto out_tmp_rel = buffer.table + "/.flush-" + to_string(CappedNowMicros()) + ".tmp.parquet";
			auto out_tmp_abs = storage.Root() + "/" + out_tmp_rel;
			if (fs.FileExists(out_tmp_abs)) {
				fs.RemoveFile(out_tmp_abs);
			}
			CheckQuery(
			    con.Query("COPY (SELECT * FROM read_parquet(" + files_lit + ")) TO " +
			              CappedQuoteLit(out_tmp_abs) + " (FORMAT parquet)"),
			    "merging fragments");
			TempFileGuard out_tmp_guard(fs, out_tmp_abs);
			int64_t mrows = 0, mbytes = 0, mmin_ts = 0, mmax_ts = 0;
			bool mhas_ts = false;
			ComputeSegmentStats(con, out_tmp_abs, time_col.Str(), mrows, mbytes, mhas_ts, mmin_ts, mmax_ts);
			fs.MoveFile(out_tmp_abs, out_abs);
			out_tmp_guard.Commit();
			// the output is unreferenced garbage until the swap below lands
			TempFileGuard out_guard(fs, out_abs);
			candidate = ApplyCompactionFfi(candidate, plan_json,
			                               FragmentJson(out_seq, out_rel, mrows, mbytes, mhas_ts, mmin_ts, mmax_ts,
			                                        merge.output_tier),
			                               CappedNowMicros() + GraceQueueOffset(catalog));
			storage.WriteManifest(fs, buffer.table, candidate);
			out_guard.Commit();
		}
	} catch (std::exception &ex) {
		// best effort: the committed fragment stands; the next flush or
		// enforce pass retries the merges (and surfaces the failure)
		record_maintenance_failure("fragment compaction", ex);
	}
	// the file-count trigger (§5 T6) and the size/row/span seal each run even
	// when the other steps failed in this pass
	try {
		int32_t frag_seal = 0;
		if (capped_manifest_needs_fragment_seal(candidate.c_str(), &frag_seal, err.Receive()) == CAPPED_OK &&
		    frag_seal == 1) {
			SealOpenUnlocked(catalog, buffer.table);
		}
	} catch (std::exception &ex) {
		record_maintenance_failure("fragment-count seal", ex);
	}
	try {
		int32_t seal = 0;
		if (capped_should_seal_default(candidate.c_str(), &seal, err.Receive()) == CAPPED_OK && seal == 1) {
			SealOpenUnlocked(catalog, buffer.table);
		}
	} catch (std::exception &ex) {
		record_maintenance_failure("seal", ex);
	}
	// the pending-delete sweep (§3.7) runs even when a step above failed in
	// this pass, and deletes nothing before its not_before
	try {
		SweepUnlocked(catalog, buffer.table);
	} catch (std::exception &ex) {
		record_maintenance_failure("pending-delete sweep", ex);
	}

	// a segment that just sealed (here or above) is now searchable; best effort,
	// an index is never worth failing a commit over
	string index_error;
	CappedIndex::EnsureUnlocked(catalog, buffer.table, index_error, CappedIndexMode::BuildMissing);
}

bool CappedWrite::TryParseGraceMicros(const string &text, int64_t &out_micros) {
	// keep the grammar single-sourced in this file's anonymous namespace
	return ParseGraceText(text, out_micros);
}

CappedEnforceSummary CappedWrite::Enforce(CappedCatalog &catalog, const string &table) {
	if (catalog.ReadOnly()) {
		return CappedEnforceSummary {}; // eviction, sweep and seal write; READ_ONLY never runs them
	}
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
	if (catalog.ReadOnly()) {
		return false; // sealing writes a segment + manifest
	}
	auto lock = catalog.LockWrites();
	if (!SealOpenUnlocked(catalog, table)) {
		return false; // no open segment (or nothing written into it yet)
	}
	// the freshly sealed segment is now evictable: rotation + caps apply to it
	EnforceUnlocked(catalog, table, false);
	// ... and searchable, if the table asked for indexes
	string index_error;
	CappedIndex::EnsureUnlocked(catalog, table, index_error, CappedIndexMode::BuildMissing);
	return true;
}

void CappedWrite::AttachMaintenance(CappedCatalog &catalog, const string &table) {
	if (catalog.ReadOnly()) {
		return; // migration, sweep and GC all write
	}
	auto &fs = catalog.FS();
	auto &storage = catalog.Storage();
	auto manifest = storage.ReadManifest(fs, table);

	// 1. lazy legacy migration (§3.8): a pre-fragment open segment holds a
	// complete Parquet file at its path, so seal it in place — same path,
	// same rows, no rewrite, no new file. Re-read after, so sweep and GC see
	// the migrated state.
	int32_t legacy = 0;
	CappedString err;
	if (capped_manifest_is_legacy_open(manifest.c_str(), &legacy, err.Receive()) != CAPPED_OK) {
		throw IOException("capped: legacy open segment check failed: %s", err.Str());
	}
	if (legacy == 1) {
		manifest = SealLegacyOpenFfi(manifest, CappedNowMicros() + GraceQueueOffset(catalog));
		storage.WriteManifest(fs, table, manifest);
	}

	// 2. pending-delete sweep: manifest first, then the files (§3.7)
	SweepUnlocked(catalog, table);

	// 3. orphan GC, only now — after the manifest is final for this ATTACH
	manifest = storage.ReadManifest(fs, table);
	OrphanGcUnlocked(catalog, table, manifest);
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
