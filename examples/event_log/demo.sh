#!/usr/bin/env bash
# Append-only event log in a capped store — no OTLP, no generated files,
# just SQL. One table, two caps doing their job in turn:
#
#   app_log   ttl='3 days' + max_bytes=<measured>
#
# Seven day-batches (6 days ago .. today) are inserted in a loop, each in its
# own transaction and sealed, so one segment == one day and retention is a
# whole-file decision. The four expired days leave by ttl; the size cap then
# drops the oldest surviving segment (a size cap has to be sized against the
# data, so a probe insert measures one sealed segment first — exactly as
# examples/telemetry does). A final partial batch is rotated by capped_seal.
#
#   ./demo.sh                     # uses ../../build/release/duckdb
#   DUCKDB=/path/to/duckdb ./demo.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
DUCKDB="${DUCKDB:-$HERE/../../build/release/duckdb}"
EXT="${EXT:-$HERE/../../build/release/extension/capped/capped.duckdb_extension}"
TMPD="${TMPDIR:-/tmp}"
WORK="${WORK:-${TMPD%/}/capped-event-log-demo}"
STORE="$WORK/store"

[[ -x "$DUCKDB" ]] || { echo "no duckdb binary at $DUCKDB (build the shell, or set DUCKDB=)"; exit 1; }
[[ -f "$EXT" ]] || { echo "extension not built: $EXT (run: GEN=ninja make release)"; exit 1; }
DUCKDB="$(cd "$(dirname "$DUCKDB")" && pwd)/$(basename "$DUCKDB")"
EXT="$(cd "$(dirname "$EXT")" && pwd)/$(basename "$EXT")"

db()      { "$DUCKDB" -unsigned -no-agent -noheader -list -c "LOAD '$EXT'; $1"; }
pretty()  { "$DUCKDB" -unsigned -no-agent -c "LOAD '$EXT'; $1"; }
show(){ echo; echo "── $1 ──────────────────────────────────────────────"; }

# Each batch lands every row at the same ts, so a segment's time bounds are
# that day and the ttl decision is exact. service/level/msg vary per row so
# the parquet file is real data, not one run-length.
insert_batch() {  # $1 = table, $2 = days ago, $3 = row count
	cat <<SQL
INSERT INTO log.main.$1
SELECT now() - INTERVAL 1 DAY * $2 AS ts,
       'svc-' || (i % 4)::VARCHAR             AS service,
       CASE WHEN i % 9 = 0 THEN 'error' ELSE 'info' END AS level,
       'event ' || i || ' ' || md5((i + $2 * 100000)::VARCHAR) AS msg
FROM range($3) t(i);
SQL
}

BATCH_ROWS=2000
PARTIAL_ROWS=800
DAYS=(6 5 4 3 2 1 0)

# ---------------------------------------------------------------- probe size
rm -rf "$WORK"; mkdir -p "$WORK"
show "measuring one sealed day-segment"
PROBE=$(db "
ATTACH '$STORE' AS log (TYPE capped, enforce_interval '0');
CREATE TABLE log.main.probe(ts TIMESTAMPTZ, service VARCHAR, level VARCHAR, msg VARCHAR)
  WITH (time_col='ts', max_bytes='1TB');
$(insert_batch probe 0 "$BATCH_ROWS")
SELECT * FROM capped_seal('log', 'probe');
SELECT coalesce(sum(bytes), 0) FROM capped_status('log', 'probe');
DROP TABLE log.main.probe;")
SEG_BYTES=$(echo "$PROBE" | tail -1 | tr -d '[:space:]')
[[ "$SEG_BYTES" =~ ^[0-9]+$ ]] && (( SEG_BYTES > 0 )) || { echo "probe failed: $PROBE"; exit 1; }
MAX_BYTES=$(( SEG_BYTES * 5 / 2 ))   # ~2.5 segments -> the oldest survivor goes
echo "  one segment ≈ ${SEG_BYTES} bytes -> max_bytes = ${MAX_BYTES}"

# ---------------------------------------------------------------- insert loop
show "insert loop: ${#DAYS[@]} day-batches, one transaction + one seal each"
LOAD="
ATTACH '$STORE' AS log (TYPE capped, enforce_interval '0');
CREATE TABLE log.main.app_log(
    ts TIMESTAMPTZ, service VARCHAR, level VARCHAR, msg VARCHAR)
  WITH (time_col='ts', ttl='3 days', max_bytes='${MAX_BYTES}');"
for d in "${DAYS[@]}"; do
	LOAD+="
-- day -$d
$(insert_batch app_log "$d" "$BATCH_ROWS")
SELECT * FROM capped_seal('log', 'app_log');"
	echo "  day -${d}: ${BATCH_ROWS} rows inserted + sealed"
done
LOAD+="
DETACH log;"
db "$LOAD" > /dev/null

SESSION="ATTACH '$STORE' AS log (TYPE capped, enforce_interval '0');"

show "capped_status after the loop (expired days are already gone)"
pretty "$SESSION
SELECT segment_id, sealed, rows, bytes,
       make_timestamp(min_ts_us)::TIMESTAMPTZ AS oldest,
       make_timestamp(max_ts_us)::TIMESTAMPTZ AS newest,
       fragments
FROM capped_status('log', 'app_log') ORDER BY segment_id;" | sed 's/^/  /'

# ---------------------------------------------------------------- capped_seal
show "capped_seal: rotate the open segment"
db "$SESSION
$(insert_batch app_log 0 "$PARTIAL_ROWS")" > /dev/null
pretty "$SESSION
SELECT segment_id, sealed, rows, bytes, fragments
FROM capped_status('log', 'app_log') ORDER BY segment_id;" | sed 's/^/  /'
echo "  (the newest rows sit in the open segment — being written, so retention"
echo "   never touches it; capped_status shows its fragments, not one sealed file)"
pretty "$SESSION
SELECT * FROM capped_seal('log', 'app_log');" | sed 's/^/  /'
pretty "$SESSION
SELECT segment_id, sealed, rows, bytes, fragments
FROM capped_status('log', 'app_log') ORDER BY segment_id;" | sed 's/^/  /'

show "what retention did: generated vs. kept"
TOTAL=$(( ${#DAYS[@]} * BATCH_ROWS + PARTIAL_ROWS ))
KEPT=$(db "$SESSION
SELECT count(*) FROM log.main.app_log;" | tail -1 | tr -d '[:space:]')
SEAL=$(db "$SESSION
SELECT count(*) FROM capped_status('log', 'app_log') WHERE sealed;" | tail -1 | tr -d '[:space:]')
echo "  generated: ${TOTAL} rows over ${#DAYS[@]}+1 batches"
echo "  kept:      ${KEPT} rows in ${SEAL} sealed segment file(s)"
echo "  forgotten: $(( TOTAL - KEPT )) rows — whole files, never row deletes"
echo "  on disk:   $(ls -1 "$STORE/app_log" | grep -v manifest.json | tr '\n' ' ')"
echo "             (a segment sealed from a single fragment keeps its fragment name;"
echo "              what retention dropped is simply no longer here)"

show "queries still have pushdown over what is left"
pretty "$SESSION
SELECT service, level, count(*) AS n, min(ts) AS first_seen
FROM log.main.app_log WHERE level = 'error' GROUP BY 1, 2 ORDER BY n DESC;" | sed 's/^/  /'

echo
echo "DEMO OK — store kept at $WORK (re-running wipes and rebuilds it)"
echo "  poke around:"
echo "    $DUCKDB -unsigned -c \"LOAD '$EXT'; ATTACH '$STORE' AS log (TYPE capped); SELECT * FROM capped_status('log', 'app_log');\""
