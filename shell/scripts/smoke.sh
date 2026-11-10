#!/usr/bin/env bash
# Smoke-test the loadable capped extension against the local duckdb CLI
# (must be the same DuckDB version the extension was built against).
#
# Usage: scripts/smoke.sh    (from capped/shell after `GEN=ninja make release`)
set -euo pipefail
cd "$(dirname "$0")/.."

EXT="$PWD/build/release/extension/capped/capped.duckdb_extension"
STORE="$(mktemp -d)/capped_store"
[[ -f "$EXT" ]] || { echo "extension not built: $EXT"; exit 1; }

run() { duckdb -unsigned -c "LOAD '$EXT'; $1"; }
scalar() { duckdb -unsigned -csv -noheader -c "LOAD '$EXT'; $1"; }

echo "== load + version =="
run "SELECT capped_version() AS core_version;"

echo "== attach + create =="
run "
ATTACH '$STORE' AS cs (TYPE capped);
CREATE TABLE cs.main.events(ts TIMESTAMPTZ, severity VARCHAR, body VARCHAR)
    WITH (time_col='ts', ttl='36500 days', max_bytes='1GB');
CREATE TABLE cs.main.metrics(ts TIMESTAMPTZ, name VARCHAR, value DOUBLE) WITH (time_col='ts', ttl='30 days');
SHOW TABLES FROM cs.main;"

echo "== manifest contents (written by capped-core) =="
cat "$STORE/events/manifest.json"
echo

echo "== persistence across re-attach =="
run "
ATTACH '$STORE' AS cs (TYPE capped);
SHOW TABLES FROM cs.main;"

echo "== uncapped table must be rejected =="
if duckdb -unsigned -c "LOAD '$EXT'; ATTACH '$STORE' AS cs (TYPE capped);
    CREATE TABLE cs.main.uncapped(x INTEGER);" 2>/dev/null; then
    echo "FAIL: uncapped table was accepted"; exit 1
else
    echo "OK: rejected"
fi

echo "== scan: empty table reads as empty (M1b) =="
C=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM cs.main.events;")
[[ "$C" == "0" ]] || { echo "FAIL: expected 0 rows, got '$C'"; exit 1; }
echo "OK: 0 rows"

echo "== scan: adopt parquet files as sealed segments =="
run "ATTACH '$STORE' AS cs (TYPE capped);
COPY (SELECT UNNEST([TIMESTAMPTZ '2026-11-07 00:00:00+00', TIMESTAMPTZ '2026-11-07 01:00:00+00']) AS ts,
             UNNEST(['info', 'error']) AS severity,
             UNNEST(['ok', 'boom']) AS body)
    TO '$STORE/events/0.seg.parquet' (FORMAT parquet);
SELECT * FROM capped_add_segment('cs', 'events', 'events/0.seg.parquet');"

C=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM cs.main.events;")
[[ "$C" == "2" ]] || { echo "FAIL: expected 2 rows, got '$C'"; exit 1; }
C=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM cs.main.events WHERE severity = 'error';")
[[ "$C" == "1" ]] || { echo "FAIL: expected 1 error row, got '$C'"; exit 1; }
C=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT body FROM cs.main.events WHERE severity = 'error';")
[[ "$C" == "boom" ]] || { echo "FAIL: expected body 'boom', got '$C'"; exit 1; }
echo "OK: 2 rows, filter pushdown returns 1 error row"

echo "== insert path: INSERT INTO + status + seal (M1c) =="
run "ATTACH '$STORE' AS cs (TYPE capped);
INSERT INTO cs.main.events VALUES (TIMESTAMPTZ '2026-11-07 02:00:00+00', 'warn', 'opened');"
C=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM cs.main.events;")
[[ "$C" == "3" ]] || { echo "FAIL: expected 3 rows after INSERT, got '$C'"; exit 1; }
SORTED=$(duckdb -unsigned -csv -noheader -c "LOAD '$EXT'; ATTACH '$STORE' AS cs (TYPE capped); SELECT sealed::VARCHAR FROM capped_status('cs','events') ORDER BY sealed;")
echo "$SORTED" | tr '\n' ' ' | grep -q "false true" && echo "OK: open + sealed segments visible" || { echo "FAIL: expected open+sealed segments, got: $SORTED"; exit 1; }
run "ATTACH '$STORE' AS cs (TYPE capped); SELECT * FROM capped_seal('cs','events');"
S=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM capped_status('cs','events') WHERE NOT sealed;")
[[ "$S" == "0" ]] || { echo "FAIL: expected 0 open segments after seal, got '$S'"; exit 1; }
C=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM cs.main.events;")
[[ "$C" == "3" ]] || { echo "FAIL: expected 3 rows after seal, got '$C'"; exit 1; }
echo "OK: seal keeps data, opens next segment"

echo "== hard cap: on_violation => block =="
run "ATTACH '$STORE' AS cs (TYPE capped);
CREATE TABLE cs.main.bounded(ts TIMESTAMPTZ, v INT) WITH (time_col='ts', max_rows='2', on_violation='block');
INSERT INTO cs.main.bounded VALUES (TIMESTAMPTZ '2026-11-07 00:00:00+00', 1), (TIMESTAMPTZ '2026-11-07 00:00:01+00', 2);"
if duckdb -unsigned -c "LOAD '$EXT'; ATTACH '$STORE' AS cs (TYPE capped);
    INSERT INTO cs.main.bounded VALUES (TIMESTAMPTZ '2026-11-07 00:00:02+00', 3);" 2>/dev/null; then
    echo "FAIL: insert over hard cap was accepted"; exit 1
else
    echo "OK: rejected"
fi
C=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM cs.main.bounded;")
[[ "$C" == "2" ]] || { echo "FAIL: bounded table should still have 2 rows, got '$C'"; exit 1; }

echo "== rollback discards buffered inserts =="
run "ATTACH '$STORE' AS cs (TYPE capped);
BEGIN TRANSACTION;
INSERT INTO cs.main.bounded VALUES (TIMESTAMPTZ '2026-11-07 00:00:03+00', 42);
ROLLBACK;" 2>/dev/null || true
C=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM cs.main.bounded;")
[[ "$C" == "2" ]] || { echo "FAIL: rollback leaked rows, got '$C'"; exit 1; }
echo "OK: rollback left nothing"

echo "== ingest-time retention (hidden _ingest_ts) =="
# no time_col => the store maintains _ingest_ts itself and rows age from the
# moment they commit; users never see or supply the column
run "ATTACH '$STORE' AS cs (TYPE capped);
CREATE TABLE cs.main.arrivals(host VARCHAR, msg VARCHAR) WITH (ttl='1 hour');
INSERT INTO cs.main.arrivals VALUES ('a', 'one'), ('b', 'two');
SELECT * FROM capped_seal('cs', 'arrivals');
SELECT * FROM capped_enforce('cs', 'arrivals');" > /dev/null
COLS=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM (DESCRIBE cs.main.arrivals);")
[[ "$COLS" == "2" ]] || { echo "FAIL: _ingest_ts leaked into the table schema ($COLS columns)"; exit 1; }
TC=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT DISTINCT time_col FROM capped_status('cs', 'arrivals');")
[[ "$TC" == "_ingest_ts" ]] || { echo "FAIL: expected the hidden time column, got '$TC'"; exit 1; }
B=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM capped_status('cs', 'arrivals') WHERE min_ts_us IS NULL OR max_ts_us IS NULL;")
[[ "$B" == "0" ]] || { echo "FAIL: committed rows reported no time bounds"; exit 1; }
C=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM cs.main.arrivals;")
[[ "$C" == "2" ]] || { echo "FAIL: fresh rows should survive enforce (got '$C')"; exit 1; }
echo "OK: rows age by arrival time, invisible to DESCRIBE and INSERT"

# a bulk file with no arrival time gets *no* bounds, so it is never age-droppable
# (a fabricated 1970 bound used to have the next enforce delete everything)
run "ATTACH '$STORE' AS cs (TYPE capped);
COPY (SELECT 'c' AS host, 'no arrival time' AS msg)
    TO '$STORE/arrivals/9.seg.parquet' (FORMAT parquet);
SELECT * FROM capped_add_segment('cs', 'arrivals', 'arrivals/9.seg.parquet');
SELECT * FROM capped_enforce('cs', 'arrivals');" > /dev/null
B=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT coalesce(min_ts_us::VARCHAR || '/' || max_ts_us::VARCHAR, 'null') FROM capped_status('cs', 'arrivals') WHERE path = 'arrivals/9.seg.parquet';")
[[ "$B" == "null" ]] || { echo "FAIL: untimed segment reported bounds '$B'"; exit 1; }
C=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM cs.main.arrivals;")
[[ "$C" == "3" ]] || { echo "FAIL: untimed segment lost rows (got '$C')"; exit 1; }
echo "OK: no arrival time => unknown bounds, rows intact"

# ... while a bulk file that carries an ancient arrival time is evicted on sight
run "ATTACH '$STORE' AS cs (TYPE capped);
COPY (SELECT 'd' AS host, 'arrived in 2020' AS msg,
             TIMESTAMPTZ '2020-01-01 00:00:00+00' AS _ingest_ts)
    TO '$STORE/arrivals/8.seg.parquet' (FORMAT parquet);
SELECT * FROM capped_add_segment('cs', 'arrivals', 'arrivals/8.seg.parquet');" > /dev/null
C=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM cs.main.arrivals;")
[[ "$C" == "3" ]] || { echo "FAIL: ancient segment should have been evicted (got '$C')"; exit 1; }
echo "OK: ancient arrival time => evicted by age"

# DuckDB's parquet reader needs one uniform schema across a table's segments, so
# a drifted file is refused instead of poisoning every later read of the table
if duckdb -unsigned -c "LOAD '$EXT'; ATTACH '$STORE' AS cs (TYPE capped);
    COPY (SELECT 1 AS a, 'x' AS b) TO '$STORE/arrivals/7.seg.parquet' (FORMAT parquet);
    SELECT * FROM capped_add_segment('cs', 'arrivals', 'arrivals/7.seg.parquet');" 2>/dev/null; then
    echo "FAIL: schema-drifted segment was adopted"; exit 1
fi
C=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM cs.main.arrivals;")
[[ "$C" == "3" ]] || { echo "FAIL: the rejected adoption disturbed the table (got '$C')"; exit 1; }
echo "OK: schema drift rejected, table still readable"

# aging on the real clock: a one-second ttl drops the rows once they are sealed
run "ATTACH '$STORE' AS cs (TYPE capped);
CREATE TABLE cs.main.stale(x INT) WITH (ttl='1 second');
INSERT INTO cs.main.stale VALUES (1), (2);" > /dev/null
sleep 2
run "ATTACH '$STORE' AS cs (TYPE capped);
SELECT * FROM capped_seal('cs', 'stale');
SELECT * FROM capped_enforce('cs', 'stale');" > /dev/null
C=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM cs.main.stale;")
[[ "$C" == "0" ]] || { echo "FAIL: rows older than the ttl survived (got '$C')"; exit 1; }
echo "OK: rows age out by arrival time"

# the name belongs to the store
if duckdb -unsigned -c "LOAD '$EXT'; ATTACH '$STORE' AS cs (TYPE capped);
    CREATE TABLE cs.main.hack(_ingest_ts TIMESTAMPTZ, v INT) WITH (max_rows='10');" 2>/dev/null; then
    echo "FAIL: _ingest_ts was accepted as a user column"; exit 1
fi
echo "OK: _ingest_ts is reserved"

echo "== rejected commit leaves no temp files =="
run "ATTACH '$STORE' AS cs (TYPE capped);
CREATE TABLE cs.main.badtime(ts VARCHAR, v INT) WITH (time_col='ts', max_rows='10');
INSERT INTO cs.main.badtime VALUES ('not a timestamp', 1);" 2>/dev/null \
    && { echo "FAIL: non-temporal time_col was accepted"; exit 1; }
LEFT=$(ls -a "$STORE/badtime" 2>/dev/null | grep -c 'flush-.*tmp.parquet' || true)
[[ "$LEFT" == "0" ]] || { echo "FAIL: $LEFT temp file(s) left after a failed commit"; exit 1; }
echo "OK: non-temporal time_col rejected, table dir clean"

echo "== M2a: capped_tables overview =="
T=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT segments, sealed, open::VARCHAR, rows FROM capped_tables('cs') WHERE table_name = 'events';")
[[ "$T" == "2,2,false,3" ]] || { echo "FAIL: capped_tables(events) expected '2,2,false,3', got '$T'"; exit 1; }
echo "OK: overview matches (2 sealed segments, 3 rows)"

echo "== M2a: capped_enforce is a no-op within budget =="
E=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT segments_dropped::VARCHAR || '/' || open_sealed::VARCHAR FROM capped_enforce('cs', 'events');")
[[ "$E" == "0/false" ]] || { echo "FAIL: capped_enforce(events) expected '0/false', got '$E'"; exit 1; }
ALL=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM capped_enforce('cs') WHERE segments_dropped > 0 OR open_sealed;")
[[ "$ALL" == "0" ]] || { echo "FAIL: capped_enforce(catalog) evicted/sealed something within budget"; exit 1; }
NAMES=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM capped_enforce('cs') WHERE table_name IN ('events', 'metrics', 'bounded');")
[[ "$NAMES" == "3" ]] || { echo "FAIL: capped_enforce(catalog) missing one of events/metrics/bounded, got '$NAMES'"; exit 1; }
echo "OK: nothing evicted, one row per table"

echo "== M2a: read-your-writes guard fails loud =="
if duckdb -unsigned -c "LOAD '$EXT'; ATTACH '$STORE' AS cs (TYPE capped);
    BEGIN TRANSACTION;
    INSERT INTO cs.main.events VALUES (TIMESTAMPTZ '2026-11-07 04:00:00+00', 'info', 'in-flight');
    SELECT count(*) FROM cs.main.events;
    ROLLBACK;" 2>/dev/null; then
    echo "FAIL: in-transaction read of inserted table was silently allowed"; exit 1
else
    echo "OK: rejected"
fi
C=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM cs.main.events;")
[[ "$C" == "3" ]] || { echo "FAIL: guarded txn leaked rows, got '$C'"; exit 1; }

echo "== M2a: ATTACH applies retention to idle stores =="
run "ATTACH '$STORE' AS cs (TYPE capped);
CREATE TABLE cs.main.idle(ts TIMESTAMPTZ, v INT) WITH (time_col='ts', ttl='1 second');
INSERT INTO cs.main.idle VALUES (TIMESTAMPTZ '2020-01-01 00:00:00+00', 1), (TIMESTAMPTZ '2020-01-01 00:00:01+00', 2);"
C=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM cs.main.idle;")
[[ "$C" == "2" ]] || { echo "FAIL: idle should still hold its open segment, got '$C'"; exit 1; }
# simulate a quiet period: expire the open segment by sealing it out-of-band
# (open segments are never evicted; a sealed expired one must go on ATTACH)
sed -i '' 's/"sealed":false/"sealed":true/' "$STORE/idle/manifest.json"
C=$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM cs.main.idle;")
[[ "$C" == "0" ]] || { echo "FAIL: ATTACH-time enforce should have dropped the expired segment, got '$C'"; exit 1; }
echo "OK: expired sealed data dropped at ATTACH"

echo "== M2b: background enforcement thread =="
run "ATTACH '$STORE' AS cs (TYPE capped);
CREATE TABLE cs.main.bg(ts TIMESTAMPTZ, v INT) WITH (time_col='ts', ttl='1 second');
INSERT INTO cs.main.bg VALUES (TIMESTAMPTZ '2020-01-01 00:00:00+00', 1), (TIMESTAMPTZ '2020-01-01 00:00:01+00', 2);"
grep -q '"sealed":false' "$STORE/bg/manifest.json" || { echo "FAIL: expected an open segment after insert"; exit 1; }
# keep a session alive (stdin via FIFO) with a 1-second enforcement tick,
# expire its open segment out-of-band, and watch the thread drop it
FIFO="$STORE.holder.fifo"
mkfifo "$FIFO"
duckdb -unsigned < "$FIFO" > /dev/null 2>&1 &
HOLDER=$!
exec 3>"$FIFO"
printf "LOAD '%s'; ATTACH '$STORE' AS cs (TYPE capped, enforce_interval '1 second'); SELECT 1;\n" "$EXT" >&3
sleep 1.5
sed -i '' 's/"sealed":false/"sealed":true/' "$STORE/bg/manifest.json"
DROPPED=no
for _ in $(seq 1 20); do
    grep -q '"segments":\[\]' "$STORE/bg/manifest.json" && { DROPPED=yes; break; }
    sleep 0.5
done
exec 3>&-
wait "$HOLDER" 2>/dev/null || true
rm -f "$FIFO"
[[ "$DROPPED" == yes ]] || { echo "FAIL: background thread never dropped the expired segment"; exit 1; }
[[ ! -e "$STORE/bg/0.seg.parquet" ]] || { echo "FAIL: segment file survived"; exit 1; }
echo "OK: thread evicted manifest entry + segment file without any client call"

echo "== drop table removes the directory =="
run "ATTACH '$STORE' AS cs (TYPE capped); DROP TABLE cs.main.metrics; DROP TABLE cs.main.events; DROP TABLE cs.main.bounded; DROP TABLE cs.main.bg; DROP TABLE cs.main.arrivals; DROP TABLE cs.main.stale; DROP TABLE cs.main.badtime;"
[[ ! -e "$STORE/metrics" ]] && echo "OK: metrics/ gone" || { echo "FAIL: metrics/ still there"; exit 1; }
[[ ! -e "$STORE/events" ]] && echo "OK: events/ gone (manifest + segments)" || { echo "FAIL: events/ still there"; exit 1; }

echo "== M2c: telemetry demo (OTLP -> capped store -> analytics) =="
DEMO_DIR="$PWD/../examples/telemetry"
LOG="$(mktemp)"
if command -v python3 >/dev/null 2>&1 && duckdb -unsigned -c "LOAD otlp;" >/dev/null 2>&1; then
    # the demo picks its own temp store; keep it under build/ so it is easy to inspect
    WORK="$PWD/build/smoke-telemetry" bash "$DEMO_DIR/demo.sh" > "$LOG" 2>&1 \
        || { tail -30 "$LOG"; echo "FAIL: telemetry demo"; exit 1; }
    grep -q "DEMO OK" "$LOG" || { tail -30 "$LOG"; echo "FAIL: telemetry demo did not finish"; exit 1; }
    # deterministic generator (SEED): the ttl table keeps exactly the recent batch
    grep -q "logs — 1 parquet file" "$LOG" \
        || { grep "parquet file" "$LOG"; echo "FAIL: ttl eviction in demo"; exit 1; }
    echo "OK: OTLP ingest, all three caps, analytics (see build/smoke-telemetry)"
else
    echo "SKIP (needs python3 and the community otlp extension)"
fi

echo "== searchable sealed segments (fts BM25 + HNSW) =="
if duckdb -unsigned -c "LOAD fts; LOAD vss;" >/dev/null 2>&1; then
    run "
ATTACH '$STORE' AS cs (TYPE capped);
CREATE TABLE cs.main.searchable(level VARCHAR, msg VARCHAR, embedding FLOAT[4])
    WITH (max_rows='100', search_columns='msg,level', vector_columns='embedding', vector_metric='cosine');
INSERT INTO cs.main.searchable VALUES
  ('error','connection timed out waiting for the payment gateway',[1.0,0.0,0.0,0.0]::FLOAT[4]),
  ('info','request completed successfully in 12ms',[0.0,1.0,0.0,0.0]::FLOAT[4]),
  ('error','panic: connection pool exhausted',[0.0,0.0,1.0,0.0]::FLOAT[4]);"
    # the open segment is never indexed: it is rewritten on every commit
    OPEN="$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM capped_status('cs','searchable') WHERE indexed;")"
    [[ "$OPEN" == "0" ]] || { echo "FAIL: the open segment should not be indexed"; exit 1; }
    run "ATTACH '$STORE' AS cs (TYPE capped); SELECT * FROM capped_seal('cs','searchable');" >/dev/null
    SEALED="$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM capped_status('cs','searchable') WHERE indexed;")"
    [[ "$SEALED" == "1" ]] || { echo "FAIL: sealing did not build the index"; exit 1; }
    [[ -f "$STORE/searchable/0.seg.idx.duckdb" ]] || { echo "FAIL: no index file next to the segment"; exit 1; }
    HIT="$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT msg FROM capped_fts('cs','searchable','payment gateway');")"
    [[ "$HIT" == connection* ]] || { echo "FAIL: fts found '$HIT'"; exit 1; }
    K="$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT count(*) FROM capped_fts('cs','searchable','connection', k := 1);")"
    [[ "$K" == "1" ]] || { echo "FAIL: k := 1 returned $K rows"; exit 1; }
    TOP="$(scalar "ATTACH '$STORE' AS cs (TYPE capped); SELECT level FROM capped_vector_search('cs','searchable',[0.0,1.0,0.0,0.0]::FLOAT[4]) LIMIT 1;")"
    [[ "$TOP" == "info" ]] || { echo "FAIL: vector search ranked '$TOP' first, expected info"; exit 1; }
    echo "OK: seal built the index; fts + vector search answer from the sidecar, scan still reads parquet"
else
    echo "SKIP (fts/vss not available: INSTALL fts; INSTALL vss)"
fi

echo
echo "ALL SMOKE CHECKS PASSED"
