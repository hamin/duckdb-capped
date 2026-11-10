//! Search sidecars: what makes a sealed segment searchable.
//!
//! Segments are plain Parquet files read by DuckDB's own reader, which is what
//! makes analytics fast — but parquet carries no inverted or vector index, and
//! `WHERE body LIKE '%timeout%'` over retained telemetry degrades to a text scan
//! of every segment. A table that opts in gets, for each **sealed** segment, a
//! sibling index file holding the columns named in [`SearchSpec`]:
//!
//! - [`SearchSpec::columns`] — text columns, indexed for BM25 full-text search
//! - [`SearchSpec::vector_columns`] — fixed-size float vectors, indexed with HNSW
//!
//! The index is derived, disposable, and **never the source of truth**: it is
//! built when a segment seals, deleted when the segment is evicted, and a failed
//! build leaves the segment alone with `SegmentMeta::index_file` = `None`. The
//! open segment has no index (it is rewritten on every commit); search covers
//! sealed segments.
//!
//! Retention caps count segment bytes only — an index is overhead, so adding one
//! to a table must not change what eviction decides.

use crate::manifest::Column;
use serde::{Deserialize, Serialize};

/// Suffix of a segment's index file, replacing the segment's own suffix.
/// `logs/2.seg.parquet` -> `logs/2.seg.idx.duckdb`.
pub const INDEX_FILE_SUFFIX: &str = "seg.idx.duckdb";

/// Distance metric for HNSW vector indexes. Names follow DuckDB's `vss`
/// extension so the shell can pass them through verbatim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum VectorMetric {
    /// Squared Euclidean (the `vss` default).
    #[serde(rename = "l2sq")]
    #[default]
    L2Sq,
    /// Cosine distance — what normalized embeddings usually want.
    #[serde(rename = "cosine")]
    Cosine,
    /// Inner product.
    #[serde(rename = "ip")]
    InnerProduct,
}

impl VectorMetric {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "l2sq" | "l2" | "euclidean" => Ok(Self::L2Sq),
            "cosine" | "cos" => Ok(Self::Cosine),
            "ip" | "inner_product" | "dot" => Ok(Self::InnerProduct),
            other => Err(format!(
                "vector_metric must be 'l2sq', 'cosine' or 'ip', got '{other}'"
            )),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::L2Sq => "l2sq",
            Self::Cosine => "cosine",
            Self::InnerProduct => "ip",
        }
    }
}

/// Which columns of a table are indexed for search, per sealed segment.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchSpec {
    /// Text columns for BM25 full-text search.
    #[serde(default)]
    pub columns: Vec<String>,
    /// Fixed-size float vector columns for approximate nearest-neighbour search.
    #[serde(default)]
    pub vector_columns: Vec<String>,
    /// Metric used by the vector indexes.
    #[serde(default)]
    pub metric: VectorMetric,
}

impl SearchSpec {
    /// Does this table want any search indexes at all?
    pub fn enabled(&self) -> bool {
        !self.columns.is_empty() || !self.vector_columns.is_empty()
    }

    /// Reject specs that cannot be built: unknown or repeated columns, names the
    /// store reserves, and vector columns that are not float arrays.
    ///
    /// Column matching is case-insensitive (DuckDB resolves unquoted identifiers
    /// that way), and the stored names are the table's own spellings.
    pub fn validate(self, table: &str, columns: &[Column]) -> Result<Self, String> {
        let resolve = |name: &str| {
            columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(name.trim()))
                .cloned()
        };

        let mut seen: Vec<String> = Vec::new();
        let mut take = |name: &str, what: &str| -> Result<Option<String>, String> {
            let name = name.trim();
            if name.is_empty() {
                return Err(format!("{what}: empty column name"));
            }
            let col = resolve(name).ok_or_else(|| {
                format!("capped table '{table}' has no column '{name}' to index for {what}")
            })?;
            if crate::is_reserved_column(&col.name) {
                return Err(format!(
                    "'{}' is maintained by the store and cannot be indexed for {what}",
                    col.name
                ));
            }
            let lowered = col.name.to_ascii_lowercase();
            if seen.contains(&lowered) {
                return Err(format!("'{}' is indexed more than once", col.name));
            }
            seen.push(lowered);
            Ok(Some(col.name))
        };

        let mut out = SearchSpec {
            columns: Vec::new(),
            vector_columns: Vec::new(),
            metric: self.metric,
        };
        let mut has_text = false;
        for name in &self.columns {
            let n = take(name, "full-text search")?.expect("checked above");
            let ty = resolve(&n).expect("resolved in take()").ty;
            if is_varchar(&ty) {
                has_text = true;
            } else if !is_display_castable(&ty) {
                return Err(format!(
                    "full-text column '{n}' has type '{ty}'; full-text search needs a VARCHAR column (a display column like TIMESTAMP/numeric is carried as text but not tokenized)"
                ));
            }
            out.columns.push(n);
        }
        if !out.columns.is_empty() && !has_text {
            return Err(format!(
                "capped table '{table}' indexes only non-text columns for full-text search; at least one VARCHAR column is tokenized, the rest are carried for display"
            ));
        }
        for name in &self.vector_columns {
            let n = take(name, "vector search")?.expect("checked above");
            let ty = resolve(&n).expect("resolved in take()").ty;
            if vector_dim(&ty).is_none() {
                return Err(format!(
                    "vector column '{n}' has type '{ty}'; vector search needs a fixed-size float array (e.g. FLOAT[8])"
                ));
            }
            out.vector_columns.push(n);
        }
        Ok(out)
    }
}

/// Is this DuckDB type string a text column? `fts` tokenizes text; anything
/// else would fail the index build, so `CREATE TABLE` says so instead.
fn is_varchar(ty: &str) -> bool {
    matches!(
        ty.trim().to_ascii_uppercase().as_str(),
        "VARCHAR" | "TEXT" | "STRING"
    )
}

/// Types that are not tokenized but may be carried through the index as
/// `CAST(col AS VARCHAR)`, so a search hit is readable without joining back
/// to the segment — a log line wants its timestamp. Restricted to types with
/// a stable, unambiguous text rendering; lists/structs/blobs are excluded.
fn is_display_castable(ty: &str) -> bool {
    matches!(
        ty.trim().to_ascii_uppercase().as_str(),
        "TIMESTAMP"
            | "TIMESTAMPTZ"
            | "TIMESTAMP WITH TIME ZONE"
            | "DATE"
            | "TIME"
            | "TIMETZ"
            | "BIGINT"
            | "INTEGER"
            | "SMALLINT"
            | "TINYINT"
            | "HUGEINT"
            | "UBIGINT"
            | "UINTEGER"
            | "USMALLINT"
            | "UTINYINT"
            | "DOUBLE"
            | "REAL"
            | "FLOAT"
            | "BOOLEAN"
            | "UUID"
    )
}

/// The dimension of a column that HNSW can index, if any. The `vss` extension
/// only builds an HNSW index on `FLOAT[N]` (a fixed-size float array), so a
/// variable-size list or a double array is rejected here rather than at seal
/// time — parquet round-trips fixed-size lists as plain lists, so the shell
/// casts them back to the declared shape when building the index.
fn vector_dim(ty: &str) -> Option<usize> {
    let t = ty.trim().to_ascii_uppercase();
    let open = t.find('[')?;
    if !t.ends_with(']') {
        return None;
    }
    if !matches!(t[..open].trim(), "FLOAT" | "REAL") {
        return None;
    }
    t[open + 1..t.len() - 1].trim().parse::<usize>().ok()
}

/// The index file that belongs to a segment file, in the same directory.
pub fn index_path_for(segment_path: &str) -> String {
    let dir = match segment_path.rfind('/') {
        Some(i) => &segment_path[..=i],
        None => "",
    };
    let stem = match segment_path.rfind('/') {
        Some(i) => &segment_path[i + 1..],
        None => segment_path,
    };
    let stem = stem
        .strip_suffix(".seg.parquet")
        .or_else(|| stem.strip_suffix(".parquet"))
        .unwrap_or(stem);
    format!("{dir}{stem}.{INDEX_FILE_SUFFIX}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::Column;

    fn cols() -> Vec<Column> {
        vec![
            Column {
                name: "ts".into(),
                ty: "TIMESTAMPTZ".into(),
            },
            Column {
                name: "body".into(),
                ty: "VARCHAR".into(),
            },
            Column {
                name: "embedding".into(),
                ty: "FLOAT[8]".into(),
            },
        ]
    }

    #[test]
    fn index_path_follows_the_segment() {
        assert_eq!(
            index_path_for("logs/2.seg.parquet"),
            "logs/2.seg.idx.duckdb"
        );
        // defensive: a segment path that did not come from the writer still
        // yields a sibling path rather than a sibling-of-nothing
        assert_eq!(index_path_for("logs/weird.parquet"), "logs/weird.seg.idx.duckdb");
    }

    #[test]
    fn spec_keeps_the_tables_own_spellings() {
        let spec = SearchSpec {
            columns: vec![" BODY ".into()],
            vector_columns: vec!["Embedding".into()],
            metric: VectorMetric::Cosine,
        }
        .validate("t", &cols())
        .unwrap();
        assert_eq!(spec.columns, vec!["body"]);
        assert_eq!(spec.vector_columns, vec!["embedding"]);
        assert_eq!(spec.metric, VectorMetric::Cosine);
        assert!(spec.enabled());
        assert!(!SearchSpec::default().enabled());
    }

    #[test]
    fn spec_rejects_columns_that_cannot_be_indexed() {
        let err = |s: SearchSpec| s.validate("t", &cols()).unwrap_err();
        assert!(err(SearchSpec {
            columns: vec!["nope".into()],
            ..Default::default()
        })
        .contains("no column 'nope'"));
        assert!(err(SearchSpec {
            columns: vec!["body".into(), "body".into()],
            ..Default::default()
        })
        .contains("indexed more than once"));
        assert!(err(SearchSpec {
            columns: vec!["".into()],
            ..Default::default()
        })
        .contains("empty column name"));
        // a VARCHAR is not a vector, a timestamp is neither, and a TIMESTAMPTZ
        // is not text: all three would fail the index build, so they are refused
        // where the user can still fix them
        assert!(err(SearchSpec {
            vector_columns: vec!["body".into()],
            ..Default::default()
        })
        .contains("needs a fixed-size float array"));
        assert!(err(SearchSpec {
            vector_columns: vec!["ts".into()],
            ..Default::default()
        })
        .contains("needs a fixed-size float array"));
        // a TIMESTAMP alone would leave BM25 nothing to tokenize
        assert!(err(SearchSpec {
            columns: vec!["ts".into()],
            ..Default::default()
        })
        .contains("at least one VARCHAR"));
        // blob has no useful text rendering at all
        let mut with_blob = cols();
        with_blob.push(Column {
            name: "raw".into(),
            ty: "BLOB".into(),
        });
        assert!(SearchSpec {
            columns: vec!["raw".into(), "body".into()],
            ..Default::default()
        }
        .validate("t", &with_blob)
        .unwrap_err()
        .contains("needs a VARCHAR column"));
        // the store's own columns are off limits — and they only show up in the
        // schema of a table that actually has one (an ingest-time table)
        let mut with_hidden = cols();
        with_hidden.push(Column {
            name: crate::policy::INGEST_TS_COLUMN.into(),
            ty: "TIMESTAMPTZ".into(),
        });
        let reserved = SearchSpec {
            columns: vec![crate::policy::INGEST_TS_COLUMN.into()],
            ..Default::default()
        }
        .validate("t", &with_hidden)
        .unwrap_err();
        assert!(reserved.contains("maintained by the store"), "got: {reserved}");
    }

    #[test]
    fn indexable_column_types() {
        assert_eq!(vector_dim("FLOAT[8]"), Some(8));
        assert_eq!(vector_dim("real[2]"), Some(2));
        assert_eq!(vector_dim(" FLOAT[1] "), Some(1));
        // vss indexes neither variable-size lists, double arrays, nor scalars
        for ty in ["FLOAT[]", "DOUBLE[8]", "FLOAT", "INTEGER[3]", "VARCHAR", "STRUCT(a INT)"] {
            assert_eq!(vector_dim(ty), None, "{ty} should not be indexable");
        }
        for ty in ["VARCHAR", "varchar", " TEXT ", "STRING"] {
            assert!(is_varchar(ty), "{ty} should be searchable text");
        }
        for ty in ["TEXT[]", "INTEGER", "BLOB", "TIMESTAMPTZ"] {
            assert!(!is_varchar(ty), "{ty} is not text");
        }
        // carried for display (cast to VARCHAR at index time) but not tokenized
        for ty in ["TIMESTAMP", "TIMESTAMPTZ", "DATE", "BIGINT", "DOUBLE", "BOOLEAN", "UUID"] {
            assert!(is_display_castable(ty), "{ty} should be a display column");
            assert!(!is_varchar(ty), "{ty} must not be tokenized");
        }
        // composite/unrenderable types stay out
        for ty in ["BLOB", "INTEGER[]", "STRUCT(a INT)", "MAP(VARCHAR, INT)"] {
            assert!(!is_display_castable(ty), "{ty} is not display-castable");
        }
    }

    #[test]
    fn display_columns_may_mingle_with_text() {
        // a log line is readable from the hit alone: ts is carried, body is
        // what BM25 actually matches on
        let spec = SearchSpec {
            columns: vec!["ts".into(), "body".into()],
            ..Default::default()
        }
        .validate("t", &cols())
        .unwrap();
        assert_eq!(spec.columns, vec!["ts", "body"]);
    }

    #[test]
    fn metric_names_match_the_vss_extension() {
        assert_eq!(VectorMetric::parse("cosine").unwrap(), VectorMetric::Cosine);
        assert_eq!(VectorMetric::parse(" L2 ").unwrap(), VectorMetric::L2Sq);
        assert_eq!(VectorMetric::parse("").unwrap(), VectorMetric::L2Sq);
        assert_eq!(VectorMetric::parse("ip").unwrap().as_str(), "ip");
        assert!(VectorMetric::parse("manhattan").is_err());
    }
}
