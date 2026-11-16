#!/usr/bin/env bash
# End-to-end demo: OTLP telemetry files -> capped store -> retention -> analytics.
#
#   ./demo.sh                     # uses ../../build/release/duckdb
#   DUCKDB=/path/to/duckdb ./demo.sh
#
# Four tables, the cap each one exercises:
#   logs            ttl='2 days'   (event time)  -- 5d-old and 3d-old batches never stay
#   logs_by_arrival ttl='2 days'   (ingest time) -- identical data, nothing dropped:
#                                                  it all *arrived* just now
#   traces          max_rows='300' (count)       -- oldest batch is dropped
#   metrics         max_bytes=...  (size)        -- sized from a probe insert, oldest dropped
#
# Each day-batch is inserted in its own transaction and sealed, so one segment
# == one day and retention is a whole-file decision.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
DUCKDB="${DUCKDB:-$HERE/../../build/release/duckdb}"
TMPD="${TMPDIR:-/tmp}"
WORK="${WORK:-${TMPD%/}/capped-telemetry-demo}"
STORE="$WORK/store"
OTLP="$WORK/otlp"

[[ -x "$DUCKDB" ]] || { echo "no duckdb binary at $DUCKDB (build the shell, or set DUCKDB=)"; exit 1; }
DUCKDB="$(cd "$(dirname "$DUCKDB")" && pwd)/$(basename "$DUCKDB")"

# -unsigned: the community otlp extension is not signed. -noheader -list: raw output for shell capture.
db()      { "$DUCKDB" -unsigned -noheader -list -c "$1"; }
pretty()  { "$DUCKDB" -unsigned -c "$1"; }
show(){ echo; echo "── $1 ──────────────────────────────────────────────"; }

# ---------------------------------------------------------------- extensions
if ! "$DUCKDB" -unsigned -c "LOAD otlp;" >/dev/null 2>&1; then
	echo "installing the otlp extension (community repository)..."
	"$DUCKDB" -unsigned -c "INSTALL otlp FROM community;" >/dev/null
fi

# ---------------------------------------------------------------- generate
rm -rf "$WORK"; mkdir -p "$OTLP"
show "generating OTLP files"
python3 "$HERE/gen_otlp.py" "$OTLP"
ls -1 "$OTLP" | sed "s|^|  |"

BATCHES=(old-5d old-3d recent)

# INSERT fragments (one per signal); $1 is the batch name.
insert_logs() {
	cat <<SQL
INSERT INTO cs.main.logs
SELECT time_unix_nano::TIMESTAMPTZ, service_name, severity_text, body, log_attributes
FROM read_otlp_logs('$OTLP/logs-$1.otlp');
SELECT * FROM capped_seal('cs', 'logs');
SQL
}
insert_logs_by_arrival() {
	# same rows as `logs`, minus the event timestamp: with no time_col the store
	# ages the table by _ingest_ts (its own commit time) instead
	cat <<SQL
INSERT INTO cs.main.logs_by_arrival
SELECT service_name, severity_text, body, log_attributes
FROM read_otlp_logs('$OTLP/logs-$1.otlp');
SELECT * FROM capped_seal('cs', 'logs_by_arrival');
SQL
}
insert_searchable_logs() {
	# same feed as `logs`, but this table opted into search: sealing it also builds
	# a BM25 index over body/severity and an HNSW index over the vector column.
	# The embedding is deliberately a toy — a word-count vector over the message,
	# standing in for a model's output — because what is being demoed is the API
	# and the retention lifecycle, not embedding quality.
	local words="['timeout','limit','failed','refused','quota','breaker','shard','query']"
	cat <<SQL
INSERT INTO cs.main.searchable_logs
SELECT time_unix_nano::TIMESTAMPTZ, service_name, severity_text, body,
       list_transform($words,
         w -> ((length(lower(body)) - length(replace(lower(body), w, ''))) / length(w))::FLOAT
       )::FLOAT[8]
FROM read_otlp_logs('$OTLP/logs-$1.otlp');
SELECT * FROM capped_seal('cs', 'searchable_logs');
SQL
}
insert_traces() {
	cat <<SQL
INSERT INTO cs.main.traces
SELECT start_time_unix_nano::TIMESTAMPTZ, service_name, name, kind, status_code,
       duration_time_unix_nano / 1e6, span_attributes
FROM read_otlp_traces('$OTLP/traces-$1.otlp');
SELECT * FROM capped_seal('cs', 'traces');
SQL
}
insert_metrics() {
	cat <<SQL
INSERT INTO cs.main.metrics
SELECT time_unix_nano::TIMESTAMPTZ, service_name, name,
       coalesce(double_value, int_value::DOUBLE), metric_attributes
FROM read_otlp_metrics_gauge('$OTLP/metrics-$1.otlp')
UNION ALL
SELECT time_unix_nano::TIMESTAMPTZ, service_name, name,
       coalesce(double_value, int_value::DOUBLE), metric_attributes
FROM read_otlp_metrics_sum('$OTLP/metrics-$1.otlp');
SELECT * FROM capped_seal('cs', 'metrics');
SQL
}

LOAD_OTLP="LOAD otlp;"
CREATE_METRICS=""

# ---------------------------------------------------------------- probe size
# A size cap has to be sized against the data, so measure what one sealed
# metrics segment actually costs using the same insert path as the demo.
show "measuring one sealed metrics segment"
PROBE=$(db "$LOAD_OTLP
ATTACH '$STORE' AS cs (TYPE capped, enforce_interval '0');
CREATE TABLE cs.main.probe(ts TIMESTAMPTZ, service VARCHAR, metric VARCHAR, value DOUBLE, attrs VARCHAR)
  WITH (time_col='ts', max_bytes='1TB');
$(insert_metrics recent | grep -v "capped_seal('cs', 'metrics')" | sed "s/cs.main.metrics/cs.main.probe/")
SELECT * FROM capped_seal('cs', 'probe');
SELECT coalesce(sum(bytes), 0) FROM capped_status('cs', 'probe');
DROP TABLE cs.main.probe;")
SEG_BYTES=$(echo "$PROBE" | tail -1 | tr -d '[:space:]')
[[ "$SEG_BYTES" =~ ^[0-9]+$ ]] && (( SEG_BYTES > 0 )) || { echo "probe failed: $PROBE"; exit 1; }
MAX_BYTES=$(( SEG_BYTES * 5 / 2 ))   # ~2.5 segments -> the oldest one goes
echo "  one segment ≈ ${SEG_BYTES} bytes -> metrics cap = ${MAX_BYTES} bytes"

# ---------------------------------------------------------------- load
show "loading three day-batches into the capped store"
LOAD="$LOAD_OTLP
ATTACH '$STORE' AS cs (TYPE capped, enforce_interval '0');

CREATE TABLE cs.main.logs(
    ts TIMESTAMPTZ, service VARCHAR, severity VARCHAR, body VARCHAR, attrs VARCHAR)
  WITH (time_col='ts', ttl='2 days');

-- same feed, no time_col: the store maintains _ingest_ts and ages rows by
-- arrival, so all three batches are "new" no matter what their logs say
CREATE TABLE cs.main.logs_by_arrival(
    service VARCHAR, severity VARCHAR, body VARCHAR, attrs VARCHAR)
  WITH (ttl='2 days');

CREATE TABLE cs.main.traces(
    ts TIMESTAMPTZ, service VARCHAR, span_name VARCHAR, kind INTEGER,
    status_code INTEGER, duration_ms DOUBLE, attrs VARCHAR)
  WITH (time_col='ts', max_rows='300');

-- the same log feed again, but this one opted into search: every sealed segment
-- also gets a BM25 index over body/severity and an HNSW index over embedding,
-- in a sidecar that is deleted as soon as retention drops its segment
CREATE TABLE cs.main.searchable_logs(
    ts TIMESTAMPTZ, service VARCHAR, severity VARCHAR, body VARCHAR, embedding FLOAT[8])
  WITH (time_col='ts', ttl='2 days', search_columns='body,severity',
        vector_columns='embedding', vector_metric='cosine');

CREATE TABLE cs.main.metrics(
    ts TIMESTAMPTZ, service VARCHAR, metric VARCHAR, value DOUBLE, attrs VARCHAR)
  WITH (time_col='ts', max_bytes='${MAX_BYTES}');
"
for b in "${BATCHES[@]}"; do
	LOAD+="
-- $b
$(insert_logs "$b")
$(insert_logs_by_arrival "$b")
$(insert_searchable_logs "$b")
$(insert_traces "$b")
$(insert_metrics "$b")
"
done
LOAD+="
DETACH cs;"
db "$LOAD" > /dev/null
echo "  loaded ${#BATCHES[@]} batches x 3 signals (each batch = one segment)"

# everything below re-opens the store — retention also runs at ATTACH
SESSION="$LOAD_OTLP
ATTACH '$STORE' AS cs (TYPE capped, enforce_interval '0');"

show "capped_tables (one row per table; the cap that drives each one)"
pretty "$SESSION
SELECT table_name, segments, rows, bytes,
       ttl_us / 3600000000 AS ttl_hours, max_bytes, max_rows
FROM capped_tables('cs') ORDER BY 1;" | sed 's/^/  /'

show "segments on disk per table (what survived retention)"
for t in logs logs_by_arrival traces metrics; do
	printf '  %s — %s parquet file(s) in %s/%s/\n' "$t" "$(ls -1 "$STORE/$t" | grep -c parquet || true)" "$STORE" "$t"
	pretty "$SESSION
	SELECT segment_id, sealed, time_col, rows, bytes,
	       make_timestamp(min_ts_us)::TIMESTAMPTZ AS oldest,
	       make_timestamp(max_ts_us)::TIMESTAMPTZ AS newest
	FROM capped_status('cs', '$t') ORDER BY segment_id;" | sed 's/^/    /'
done

show "capped_enforce (retention is already applied at every write + ATTACH)"
pretty "$SESSION
SELECT table_name, segments_dropped, open_sealed FROM capped_enforce('cs') ORDER BY 1;" | sed 's/^/  /'

show "retention: what was generated vs. what the store kept"
pretty "$SESSION
WITH gen(t, n) AS (
  SELECT 'logs',            count(*) FROM read_otlp_logs('$OTLP/logs-*.otlp')
   -- identical input, aged by arrival instead of by event time
   UNION ALL SELECT 'logs_by_arrival', count(*) FROM read_otlp_logs('$OTLP/logs-*.otlp')
   UNION ALL SELECT 'traces',  count(*) FROM read_otlp_traces('$OTLP/traces-*.otlp')
   UNION ALL SELECT 'metrics', (SELECT count(*) FROM read_otlp_metrics_gauge('$OTLP/metrics-*.otlp'))
                             + (SELECT count(*) FROM read_otlp_metrics_sum('$OTLP/metrics-*.otlp'))
), kept(t, n) AS (
  SELECT 'logs', count(*) FROM cs.main.logs
  UNION ALL SELECT 'logs_by_arrival', count(*) FROM cs.main.logs_by_arrival
  UNION ALL SELECT 'traces', count(*) FROM cs.main.traces
  UNION ALL SELECT 'metrics', count(*) FROM cs.main.metrics
)
SELECT gen.t AS table_name, gen.n AS generated, kept.n AS kept, gen.n - kept.n AS dropped
FROM gen JOIN kept USING (t) ORDER BY 1;" | sed 's/^/  /'
echo "  (logs aged by event time, logs_by_arrival by the store's _ingest_ts: same"
echo "   data, and only one of them is 5 days old — see capped_status time_col above)"

show "analytics: error / fatal log rate by service (last 2 days)"
pretty "$SESSION
SELECT service,
       count(*) AS entries,
       count(*) FILTER (severity IN ('ERROR','FATAL')) AS bad,
       round(100.0 * count(*) FILTER (severity IN ('ERROR','FATAL')) / count(*), 1) AS pct_bad
FROM cs.main.logs GROUP BY 1 ORDER BY bad DESC;" | sed 's/^/  /'

show "analytics: server-span latency by service (p50 / p95 / error rate)"
pretty "$SESSION
SELECT service,
       count(*) AS spans,
       round(quantile_cont(duration_ms, 0.5), 1) AS p50_ms,
       round(quantile_cont(duration_ms, 0.95), 1) AS p95_ms,
       count(*) FILTER (status_code = 2) AS failed
FROM cs.main.traces WHERE kind = 2 GROUP BY 1 ORDER BY p95_ms DESC;" | sed 's/^/  /'

show "analytics: request volume and avg latency from metrics"
pretty "$SESSION
SELECT service,
       round(sum(value) FILTER (metric = 'http.server.request.count')) AS requests,
       round(avg(value) FILTER (metric = 'http.server.duration'), 1)   AS avg_ms
FROM cs.main.metrics GROUP BY 1 ORDER BY requests DESC;" | sed 's/^/  /'

show "cross-signal: is the busiest service the sickest one?"
pretty "$SESSION
WITH l AS (SELECT service, count(*) FILTER (severity IN ('ERROR','FATAL')) AS errors
           FROM cs.main.logs GROUP BY 1),
     t AS (SELECT service, round(quantile_cont(duration_ms, 0.95)) AS p95_ms
           FROM cs.main.traces WHERE kind = 2 GROUP BY 1),
     m AS (SELECT service, sum(value) AS requests FROM cs.main.metrics
           WHERE metric = 'http.server.request.count' GROUP BY 1)
SELECT m.service, m.requests::BIGINT AS requests, l.errors, t.p95_ms
FROM m JOIN l USING (service) JOIN t USING (service)
ORDER BY l.errors DESC;" | sed 's/^/  /'

show "filtered query (predicates go into the parquet row-group stats)"
pretty "$SESSION
SELECT service, severity, count(*) AS n, min(ts) AS first_seen
FROM cs.main.logs WHERE severity IN ('ERROR','FATAL') AND service IN ('billing','search')
GROUP BY 1, 2 ORDER BY n DESC;" | sed 's/^/  /'

show "search over the sealed segments (BM25 + HNSW live beside each segment)"
if "$DUCKDB" -unsigned -c "LOAD fts; LOAD vss;" >/dev/null 2>&1; then
	# ttl has already dropped the two old batches, so search only sees what
	# retention left — an index can never outlive its rows
	pretty "$SESSION
SELECT severity, left(body, 46) AS body, round(score, 2) AS bm25
FROM capped_fts('cs', 'searchable_logs', 'upstream timeout', k := 3)
ORDER BY bm25 DESC;" | sed 's/^/  /'
	pretty "$SESSION
SELECT left(body, 46) AS body, round(score, 3) AS cosine_distance
FROM capped_vector_search('cs', 'searchable_logs',
                          [1,0,0,0,0,0,0,0]::FLOAT[8], k := 3);" | sed 's/^/  /'
	pretty "$SESSION
SELECT count(*) AS segments, count(*) FILTER (indexed) AS indexed,
       sum(index_bytes) AS index_bytes
FROM capped_status('cs', 'searchable_logs');" | sed 's/^/  /'
	echo "  (indexes are derived data: built at seal, dropped with the segment,"
	echo "   rebuildable with capped_index('cs','searchable_logs', rebuild => true))"
else
	echo "  (skipped: capped_fts / capped_vector_search need the fts and vss extensions)"
fi

echo
echo "DEMO OK — store kept at $WORK (re-running wipes and rebuilds it)"
echo "  poke around:"
echo "    $DUCKDB -unsigned -c \"LOAD otlp; ATTACH '$WORK/store' AS cs (TYPE capped); SELECT * FROM capped_tables('cs');\""
