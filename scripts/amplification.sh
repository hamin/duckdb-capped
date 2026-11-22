#!/usr/bin/env bash
# Write-amplification gate for the open-segment fragment path (docs/open-segments.md).
#
# 720 separately committed INSERT batches of ~2 KB raw row data each —
# lotel's 5-second commit cadence over an hour — then capped_seal. Two gates
# on what the fragment path does to the DATA (see the gate block below for
# the full reasoning): row amplification (total rows across all written
# files / sealed rows) <= 4.0, and bytes_written / sealed bytes <= 6.0 —
# the byte gate is looser than the rewrite count because small fragments
# compress ~2x worse than the sealed segment, a Parquet penalty rather than
# an extra rewrite. The old whole-open-segment rewrite is estimated for the
# record at ~(N+1)/2 (≈360x).
#
# Usage: scripts/amplification.sh   (after `GEN=ninja make release`)
set -euo pipefail
cd "$(dirname "$0")/.."

DB="$PWD/build/release/duckdb"
EXT="$PWD/build/release/extension/capped/capped.duckdb_extension"
[[ -x "$DB" ]]  || { echo "duckdb not built: $DB"; exit 1; }
[[ -f "$EXT" ]] || { echo "extension not built: $EXT"; exit 1; }

STORE="$(mktemp -d)/amp_store"
BATCHES=720
ROWS=28   # 28 rows x (ts 8B + svc ~12B + 64B body + v 8B) ~= 2 KB raw per batch

# enforce_interval '0': no background thread, so the seal and every sweep
# happen exactly where the script puts them (the numbers must be reproducible)
SQL_FILE="$(mktemp)"
ERR_LOG="$(mktemp)"
trap 'rm -f "$SQL_FILE" "$ERR_LOG"' EXIT
{
    echo "LOAD '$EXT';"
    echo "ATTACH '$STORE' AS amp (TYPE capped, enforce_interval '0');"
    # a capped table needs at least one cap; max_bytes='1GB' is the default byte cap.
    # ts is a plain data column: the table is ingest-time (no time_col), so the
    # amplification path never reads it.
    echo "CREATE TABLE amp.main.logs(ts TIMESTAMP, svc VARCHAR, body VARCHAR, v BIGINT) WITH (max_bytes='1GB');"
    for ((b = 0; b < BATCHES; b++)); do
        echo "INSERT INTO amp.main.logs SELECT epoch_ms(1762000000000 + (i * 5000)::BIGINT)," \
             "'service-' || (i % 5)::VARCHAR, repeat('log ', 16), i FROM range($((b * ROWS)), $(((b + 1) * ROWS))) t(i);"
    done
    echo "SELECT * FROM capped_seal('amp', 'logs');"
    echo "SELECT 'rows=' || count(*) FROM amp.main.logs;"
    echo "SELECT rows, bytes, bytes_written FROM capped_tables('amp') WHERE table_name = 'logs';"
} > "$SQL_FILE"

echo "== write amplification gate =="
echo "store:   $STORE"
echo "commits: $BATCHES separate INSERT batches of ~2 KB raw, then capped_seal"
echo

if ! OUT="$("$DB" -unsigned -batch -csv -noheader < "$SQL_FILE" 2>"$ERR_LOG")"; then
    echo "duckdb run failed; first errors (full stderr in $ERR_LOG):"
    head -10 "$ERR_LOG"
    exit 1
fi
trap - EXIT
[[ -s "$ERR_LOG" ]] && { echo "stderr (first lines):"; head -5 "$ERR_LOG"; }
echo "$OUT" | grep -v ',' | head -5

BW_LINE="$(echo "$OUT" | grep -E '^[0-9]+,[0-9]+,[0-9]+$' | tail -1)"
TABLE_ROWS="${BW_LINE%%,*}"                       # rows in the sealed table
REST="${BW_LINE#*,}"
SEALED_BYTES="${REST%,*}"                          # compressed payload of the sealed segment
BYTES_WRITTEN="${REST#*,}"                         # every payload byte the fragment path ever wrote
[[ -n "$TABLE_ROWS" && -n "$SEALED_BYTES" && -n "$BYTES_WRITTEN" ]] || { echo "could not read capped_tables stats"; exit 1; }

FRAG="$STORE/logs/0.frag-000000.parquet"
FRAG_SIZE="$(wc -c < "$FRAG" | tr -d ' ')"   # still on disk: merged-away inputs wait out the delete grace

# Per-tier payload table. The merged-away inputs are still on disk (they wait
# out the 5-minute delete grace and no sweep runs mid-script), so what every
# write ever cost is readable from parquet_metadata: each file's compressed
# column payload and its row count (num_values / column count). Files cluster
# into tiers by rows-per-file — 28, then 8x, then 64x, ... — with the seal
# file last; ratio is payload / sealed bytes, so the column sums to the total.
ANALYSIS="$("$DB" -unsigned -batch -csv -noheader -c "
WITH chunks AS (
    SELECT file_name, path_in_schema, num_values, total_compressed_size
    FROM parquet_metadata('$STORE/logs/*.parquet')
),
per_file AS (
    SELECT file_name,
           (sum(num_values) / count(DISTINCT path_in_schema))::BIGINT AS nrows,
           sum(total_compressed_size)::BIGINT AS payload
    FROM chunks GROUP BY file_name
)
SELECT nrows, count(*), sum(payload), max(file_name) FROM per_file GROUP BY nrows ORDER BY nrows;
" 2>>"$ERR_LOG")" || { echo "analysis query failed (full stderr in $ERR_LOG):"; head -10 "$ERR_LOG"; exit 1; }

# Gate on what the fragment path actually does to the data, not on Parquet
# side effects:
#   * row amplification — total rows across all written files / sealed rows —
#     must be <= 4: each row is written once, then re-emitted per compaction
#     tier (log_8 720 ~ 3.1) plus the seal, and no row may ride more tiers.
#   * the byte ratio is allowed up to 6.0 because it carries a compression
#     penalty on top of the rewrites: a 28-row fragment compresses ~2x worse
#     than the sealed segment (~20 B/row vs ~10.6 B/row — small row groups
#     give the column codecs barely any history to exploit). Tier-0 alone
#     lands near 1.9x of the sealed bytes without rewriting a single row
#     twice, so a byte-only gate at 5x measured compression quality, not
#     amplification.
echo "bytes_written : $BYTES_WRITTEN"
echo "bytes sealed  : $SEALED_BYTES (rows: $TABLE_ROWS)"
echo "files_written : not exposed by capped_tables (only bytes_written / pending_delete_files)"
echo "context       : one tier-0 fragment ($BATCHES commits' first, $ROWS rows) is ${FRAG_SIZE} bytes on disk"
echo "                (Parquet footer + compression overhead on a ~2 KB batch)"
echo
GATE=0; awk -v sealed="$SEALED_BYTES" -v bw="$BYTES_WRITTEN" -v rows_sealed="$TABLE_ROWS" '
BEGIN { FS=","
        printf "per-tier payload / sealed bytes (%d):\n", sealed
        printf "  %-5s %6s %11s %11s %7s\n", "tier", "files", "rows/file", "payload", "ratio" }
{ files[$1] = $2; payload[$1] = $3; sample[$1] = $4
  if (minrows == "" || $1 + 0 < minrows + 0) minrows = $1
  total_rows += $1 * $2; total_payload += $3 }
END {
    n = 0; for (r in files) ord[n++] = r + 0
    for (i = 0; i < n; i++) for (j = i + 1; j < n; j++)
        if (ord[j] < ord[i]) { t = ord[i]; ord[i] = ord[j]; ord[j] = t }
    for (i = 0; i < n; i++) {
        r = ord[i]
        label = (index(sample[r], ".seg") > 0) ? "seal" : sprintf("%d", int(log(r / minrows) / log(8) + 0.5))
        printf "  %-5s %6d %11d %11d %7.2f\n", label, files[r], r, payload[r], payload[r] / sealed
    }
    printf "  %-5s %6s %11s %11d %7.2f\n", "total", "", "", total_payload, total_payload / sealed
    ra = total_rows / rows_sealed
    br = bw / sealed
    printf "\n"
    printf "row amplification : %d / %d = %.2f   (gate 4.0)\n", total_rows, rows_sealed, ra
    printf "bytes ratio       : %d / %d = %.2f   (gate 6.0)\n", bw, sealed, br
    exit (ra > 4.000001 || br > 6.000001) ? 1 : 0
}' <<< "$ANALYSIS" || GATE=1
echo
echo "old algorithm : $(awk -v n="$BATCHES" 'BEGIN{printf "%.1f", (n+1)/2}')x  [estimate, for the record: it rewrote the whole open"
echo "                segment per commit, i.e. sum over i=1..$BATCHES of (i/$BATCHES x sealed) / sealed = ($BATCHES+1)/2]"
echo

if [ "$GATE" -ne 0 ]; then
    echo "FAIL: a gate was exceeded (row amplification > 4.0 or bytes ratio > 6.0)"
    exit 1
fi
echo "PASS: row amplification <= 4.0 and bytes ratio <= 6.0"
