#!/usr/bin/env python3
"""Generate OTLP wire-format telemetry files — no third-party dependencies.

Writes protobuf-encoded ExportLogsServiceRequest / ExportMetricsServiceRequest /
ExportTraceServiceRequest files, exactly what the OpenTelemetry Collector's
file exporter produces, so duckdb-otlp's read_otlp_logs / read_otlp_metrics /
read_otlp_traces can read them directly.

Field numbers follow opentelemetry-proto v1 (logs/v1, metrics/v1, trace/v1,
common/v1). Timestamps are relative to "now" so the demo always has fresh +
expired data against a policy like ttl='2 days'.

Usage: python3 gen_otlp.py <output_dir>
"""

import random
import struct
import sys
import time
from pathlib import Path

# --- protobuf wire format (just enough for the OTLP subset we emit) ----------


def varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | 0x80 if n else b)
        if not n:
            return bytes(out)


def tag(field: int, wire: int) -> bytes:
    return varint((field << 3) | wire)


def f_varint(field: int, value: int) -> bytes:
    return tag(field, 0) + varint(value)


def f_bytes(field: int, value: bytes) -> bytes:
    return tag(field, 2) + varint(len(value)) + value


def f_str(field: int, value: str) -> bytes:
    return f_bytes(field, value.encode("utf-8"))


def f_fixed64(field: int, value: int) -> bytes:
    return tag(field, 1) + struct.pack("<Q", value)


def f_double(field: int, value: float) -> bytes:
    return tag(field, 1) + struct.pack("<d", value)


def any_value(value) -> bytes:
    """common.AnyValue: 1=string 2=bool 3=int 4=double."""
    if isinstance(value, bool):
        return f_varint(2, int(value))
    if isinstance(value, int):
        return f_varint(3, value)
    if isinstance(value, float):
        return f_double(4, value)
    return f_str(1, str(value))


def kv(key: str, value) -> bytes:
    """common.KeyValue: 1=key, 2=value(AnyValue). Returns message bytes."""
    return f_str(1, key) + f_bytes(2, any_value(value))


def resource(attrs: dict) -> bytes:
    """resource.Resource: 1=attributes (repeated KeyValue)."""
    return b"".join(f_bytes(1, kv(k, v)) for k, v in attrs.items())


def scope(name: str, version: str = "1.0.0") -> bytes:
    """common.InstrumentationScope: 1=name, 2=version."""
    return f_str(1, name) + f_str(2, version)


# --- logs (logs/v1) -----------------------------------------------------------


def log_record(ts_ns: int, sev_num: int, sev_text: str, body: str, attrs: dict) -> bytes:
    """LogRecord: 1=time(fixed64) 2=severity_number 3=severity_text
    5=body(AnyValue) 6=attributes(repeated KeyValue)."""
    b = f_fixed64(1, ts_ns)
    if sev_num:
        b += f_varint(2, sev_num)
    if sev_text:
        b += f_str(3, sev_text)
    if body:
        b += f_bytes(5, f_str(1, body))
    for k, v in attrs.items():
        b += f_bytes(6, kv(k, v))
    return b


def logs_file(batches: list) -> bytes:
    """ExportLogsServiceRequest{1: [ResourceLogs{1:resource, 2:[ScopeLogs{
    1:scope, 2:[LogRecord]}]}]}. batches = [(resource_attrs, scope_name, records)]."""
    out = b""
    for res_attrs, scope_name, records in batches:
        sl = f_bytes(1, scope(scope_name))
        sl += b"".join(f_bytes(2, r) for r in records)
        rl = f_bytes(1, resource(res_attrs)) + f_bytes(2, sl)
        out += f_bytes(1, rl)
    return out


# --- traces (trace/v1) --------------------------------------------------------


def span_bytes(trace_id: bytes, span_id: bytes, name: str, kind: int, start_ns: int, end_ns: int,
               attrs: dict, code: int = 0, message: str = "", parent: bytes = b"") -> bytes:
    """Span: 1=trace_id 2=span_id 4=parent_span_id 5=name 6=kind
    7=start(fixed64) 8=end(fixed64) 9=attributes 15=status."""
    b = f_bytes(1, trace_id) + f_bytes(2, span_id)
    if parent:
        b += f_bytes(4, parent)
    b += f_str(5, name) + f_varint(6, kind) + f_fixed64(7, start_ns) + f_fixed64(8, end_ns)
    for k, v in attrs.items():
        b += f_bytes(9, kv(k, v))
    if code or message:
        status = (f_str(2, message) if message else b"") + (f_varint(3, code) if code else b"")
        b += f_bytes(15, status)
    return b


def traces_file(batches: list) -> bytes:
    """batches = [(resource_attrs, scope_name, spans)]."""
    out = b""
    for res_attrs, scope_name, spans in batches:
        sp = f_bytes(1, scope(scope_name))
        sp += b"".join(f_bytes(2, s) for s in spans)
        rs = f_bytes(1, resource(res_attrs)) + f_bytes(2, sp)
        out += f_bytes(1, rs)
    return out


# --- metrics (metrics/v1) -----------------------------------------------------


def number_dp(ts_ns: int, value: float, attrs: dict) -> bytes:
    """NumberDataPoint: 3=time(fixed64) 4=as_double(double) 7=attributes."""
    b = f_fixed64(3, ts_ns) + f_double(4, value)
    for k, v in attrs.items():
        b += f_bytes(7, kv(k, v))
    return b


def metric_gauge(name: str, unit: str, points: list) -> bytes:
    """Metric: 1=name 3=unit 5=gauge; Gauge: 1=data_points."""
    body = b"".join(f_bytes(1, p) for p in points)
    return f_str(1, name) + f_str(3, unit) + f_bytes(5, body)


def metric_sum(name: str, unit: str, points: list, monotonic: bool = True) -> bytes:
    """Metric: 1=name 3=unit 7=sum; Sum: 1=data_points 2=temporality 3=monotonic."""
    body = b"".join(f_bytes(1, p) for p in points)
    body += f_varint(2, 2)  # cumulative
    if monotonic:
        body += f_varint(3, 1)
    return f_str(1, name) + f_str(3, unit) + f_bytes(7, body)


def metrics_file(batches: list) -> bytes:
    """batches = [(resource_attrs, scope_name, metrics)]."""
    out = b""
    for res_attrs, scope_name, metrics in batches:
        sm = f_bytes(1, scope(scope_name))
        sm += b"".join(f_bytes(2, m) for m in metrics)
        rm = f_bytes(1, resource(res_attrs)) + f_bytes(2, sm)
        out += f_bytes(1, rm)
    return out


# --- synthetic telemetry ------------------------------------------------------

SERVICES = {
    "api-gateway": ["request routed", "rate limit hit", "upstream timeout", "circuit breaker open"],
    "auth-service": ["token issued", "login failed", "session refreshed"],
    "billing": ["invoice generated", "charge failed", "refund processed"],
    "search": ["index rebuilt", "query served", "shard rebalanced"],
}
# (severity_text, severity_number, pick weight)
LEVELS = [("INFO", 9, 0.72), ("WARN", 13, 0.16), ("ERROR", 17, 0.10), ("FATAL", 21, 0.02)]
SEED = 42


def gen_logs_batches(rng: random.Random, base_ts_ns: int, per_service: int) -> list:
    """One ResourceLogs per service; timestamps base + i seconds (tight, so a
    batch's max_ts is deterministic for retention demos)."""
    batches = []
    seq = 0
    for service, pool in SERVICES.items():
        records = []
        for i in range(per_service):
            level, sev_num = rng.choices(LEVELS, weights=[w for _, _, w in LEVELS])[0][:2]
            body = rng.choice(pool)
            if level in ("ERROR", "FATAL"):
                body = rng.choice(["connection refused", "upstream timeout", "disk quota exceeded"])
            ts = base_ts_ns + i * 1_000_000_000  # 1s apart
            records.append(log_record(
                ts, sev_num, level, f"[{service}] {body} (seq {seq})",
                {"service.name": service, "request.id": f"{rng.getrandbits(32):08x}",
                 "host.name": f"node-{rng.randint(1, 8)}"},
            ))
            seq += 1
        batches.append(({"service.name": service, "deployment.environment.name": "demo"},
                        "com.example.telemageddon", records))
    return batches


def gen_trace_batches(rng: random.Random, base_ts_ns: int, per_service: int) -> list:
    batches = []
    for service in SERVICES:
        spans = []
        for i in range(per_service):
            trace_id = rng.getrandbits(128).to_bytes(16, "big")
            root_id = rng.getrandbits(64).to_bytes(8, "big")
            dur_us = rng.randint(1_000, 4_000_000)
            start = base_ts_ns + i * 7_000_000_000  # 7s apart
            spans.append(span_bytes(
                trace_id, root_id, f"GET /{rng.choice(['items', 'users', 'search', 'checkout'])}",
                2, start, start + dur_us * 1000,  # SERVER
                {"service.name": service, "http.request.method": "GET",
                 "http.response.status_code": rng.choices([200, 404, 500], weights=[90, 6, 4])[0]},
                code=2 if rng.random() < 0.05 else 1,
            ))
            if rng.random() < 0.6:  # child span (DB call)
                child_id = rng.getrandbits(64).to_bytes(8, "big")
                c_dur = rng.randint(200, max(300, dur_us // 2))
                spans.append(span_bytes(
                    trace_id, child_id, "SELECT postgres", 3,  # CLIENT
                    start + dur_us * 300, start + dur_us * 300 + c_dur * 1000,
                    {"service.name": service, "db.system.name": "postgresql"},
                    parent=root_id,
                ))
        batches.append(({"service.name": service, "deployment.environment.name": "demo"},
                        "com.example.tracing", spans))
    return batches


def gen_metric_batches(rng: random.Random, base_ts_ns: int, points: int) -> list:
    batches = []
    for service in SERVICES:
        duration_pts, request_pts = [], []
        for i in range(points):
            ts = base_ts_ns + i * 3_600 * 1_000_000_000  # hourly
            duration_pts.append(number_dp(ts, rng.uniform(5, 900), {"service.name": service}))
            request_pts.append(number_dp(ts, float(rng.randint(50, 5000)), {"service.name": service}))
        metrics = [
            metric_gauge("http.server.duration", "ms", duration_pts),
            metric_sum("http.server.request.count", "1", request_pts),
        ]
        batches.append(({"service.name": service, "deployment.environment.name": "demo"},
                        "com.example.metrics", metrics))
    return batches


def main() -> None:
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    rng = random.Random(SEED)
    now_ns = int(time.time() * 1_000_000_000)
    day_ns = 86_400 * 1_000_000_000

    # three "days" of history: ~5d and ~3d old are expired for a ttl='2 days'
    # policy; the last batch is ~1h old and must survive
    days = [("old-5d", now_ns - 5 * day_ns), ("old-3d", now_ns - 3 * day_ns),
            ("recent", now_ns - 3_600 * 1_000_000_000)]

    for name, base in days:
        (out / f"logs-{name}.otlp").write_bytes(logs_file(gen_logs_batches(rng, base, 100)))
        (out / f"traces-{name}.otlp").write_bytes(traces_file(gen_trace_batches(rng, base, 20)))
        (out / f"metrics-{name}.otlp").write_bytes(metrics_file(gen_metric_batches(rng, base, 12)))

    print(f"wrote {len(list(out.glob('*.otlp')))} OTLP files to {out}")


if __name__ == "__main__":
    main()
