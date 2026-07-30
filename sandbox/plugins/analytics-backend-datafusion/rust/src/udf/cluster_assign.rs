/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `cluster_assign(message, reps_json, t, match, delims)` — faithful cosine
//! nearest-representative label (PPL `cluster` V1, increment ②: real shared tokenizer).
//!
//! Given a document text `message` (column), a **literal** JSON array `reps_json`
//! of K representative **event texts**, a **literal** cosine threshold `t` (0 < t < 1),
//! a **literal** match mode `match` (`termlist` | `termset` | `ngramset`), and a
//! **literal** `delims` spec, returns the **1-based** index (`1..=K`) of the
//! representative with the highest cosine similarity to the document, or `-1`
//! (UNMATCHED) when the best similarity is `< t`.
//!
//! Tokenization/vectorization mirrors the exact-mode command kernel
//! (`org.opensearch.sql.common.cluster.TextFeatures` + `TextSimilarityClustering`):
//! `delims` splitting (default `non-alphanumeric` = runs of `[^A-Za-z0-9_]`),
//! purely-numeric tokens normalized to `*`, and three match modes producing
//! **TF (count-weighted)** sparse vectors — TERMLIST is positional (`"<i>-<tok>"`),
//! TERMSET is a bag of words, NGRAMSET is character trigrams. Cosine is standard
//! sparse cosine over those TF vectors (NOT binary — the exact command is TF).
//!
//! This Rust UDF and the Java `ClusterAssignFunction` oracle re-implement the same
//! tokenizer independently and MUST stay in lock-step (§4.7 FP-parity); the manual
//! split emulates Java `String.split("[delims]+")` (leading empty preserved, trailing
//! removed, consecutive delimiters collapsed) so positional keys agree exactly.
//! Bound to Substrait by name-match with the Java `ClusterAssignAdapter` +
//! `opensearch_scalar_functions.yaml` entry `cluster_assign`.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int32Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

/// V1 hard cap on the number of representatives (LLD §4.8 `k <= 50`).
const MAX_REPS: usize = 50;

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(ClusterAssignUdf::new()));
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MatchMode {
    TermList,
    TermSet,
    NgramSet,
}

fn parse_mode(s: &str) -> Result<MatchMode> {
    match s.to_ascii_lowercase().as_str() {
        "termlist" => Ok(MatchMode::TermList),
        "termset" => Ok(MatchMode::TermSet),
        "ngramset" => Ok(MatchMode::NgramSet),
        other => exec_err!(
            "cluster_assign: invalid match mode [{other}] (must be termlist|termset|ngramset)"
        ),
    }
}

/// `cluster_assign(varchar message, varchar reps_json, double t, varchar match, varchar delims)` → `int`.
#[derive(Debug)]
pub struct ClusterAssignUdf {
    signature: Signature,
}

impl ClusterAssignUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Float64,
                    DataType::Utf8,
                    DataType::Utf8,
                ])],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for ClusterAssignUdf {
    fn default() -> Self {
        Self::new()
    }
}

// `ScalarUDFImpl` requires DynEq + DynHash. Stateless — all instances equivalent.
impl PartialEq for ClusterAssignUdf {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for ClusterAssignUdf {}
impl Hash for ClusterAssignUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "cluster_assign".hash(state);
    }
}

fn scalar_utf8<'a>(v: &'a ColumnarValue, arg: &str) -> Result<&'a str> {
    match v {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => Ok(s.as_str()),
        other => exec_err!("cluster_assign: {arg} must be a non-null Utf8 literal, got {other:?}"),
    }
}

impl ScalarUDFImpl for ClusterAssignUdf {
    fn name(&self) -> &str {
        "cluster_assign"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 5 {
            return exec_err!(
                "cluster_assign expects 5 arguments (message, reps_json, t, match, delims), got {}",
                args.args.len()
            );
        }
        let n = args.number_rows;

        let reps_json = scalar_utf8(&args.args[1], "reps_json")?.to_string();
        let t = match &args.args[2] {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => *v,
            other => {
                return exec_err!(
                    "cluster_assign: t must be a non-null Float64 literal, got {other:?}"
                )
            }
        };
        // Match the exact command's validateThreshold: 0 < t < 1.
        if !(t > 0.0 && t < 1.0) {
            return exec_err!("cluster_assign: threshold must be > 0.0 and < 1.0, got {t}");
        }
        let mode = parse_mode(scalar_utf8(&args.args[3], "match")?)?;
        let delims = scalar_utf8(&args.args[4], "delims")?.to_string();

        // reps are representative EVENT TEXTS; vectorize with the same tokenizer as docs.
        let reps_texts = parse_reps(&reps_json)?;
        if reps_texts.is_empty() || reps_texts.len() > MAX_REPS {
            return exec_err!(
                "cluster_assign: reps count must be 1..={MAX_REPS}, got {}",
                reps_texts.len()
            );
        }
        let rep_vecs: Vec<HashMap<String, f64>> =
            reps_texts.iter().map(|r| vectorize(r, mode, &delims)).collect();
        let rep_norms: Vec<f64> = rep_vecs.iter().map(l2norm).collect();
        let rep_empty: Vec<bool> = reps_texts.iter().map(|r| r.is_empty()).collect();

        let msg_arr = args.args[0].clone().into_array(n)?;
        let messages = msg_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "cluster_assign: message must be Utf8, got {:?}",
                    msg_arr.data_type()
                ))
            })?;

        let mut out = Int32Array::builder(n);
        for i in 0..n {
            // Mirror exact-mode computeSimilarity: null text normalizes to "".
            let text = if messages.is_null(i) { "" } else { messages.value(i) };
            let doc = vectorize(text, mode, &delims);
            let doc_norm = l2norm(&doc);
            let doc_empty = text.is_empty();
            out.append_value(assign(
                &doc, doc_norm, doc_empty, &rep_vecs, &rep_norms, &rep_empty, t,
            ));
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
    }
}

/// Parse the reps JSON literal: a JSON array of representative event-text strings.
fn parse_reps(s: &str) -> Result<Vec<String>> {
    serde_json::from_str::<Vec<String>>(s)
        .map_err(|e| DataFusionError::Execution(format!("cluster_assign: invalid reps_json ({e})")))
}

/// Is `c` a delimiter under the given spec? `non-alphanumeric` = any char not in
/// `[A-Za-z0-9_]`; otherwise a delimiter is any char present in `delims`.
fn is_delim(c: char, delims: &str) -> bool {
    if delims == "non-alphanumeric" {
        !(c.is_ascii_alphanumeric() || c == '_')
    } else {
        delims.contains(c)
    }
}

/// Tokenize emulating Java `value.split("[delims]+")` (limit 0): consecutive
/// delimiters collapse to one boundary, a leading delimiter run yields a single
/// leading empty token (so positional keys shift by +1), trailing empties are
/// dropped. Returned tokens may include that one leading `""` (callers skip empties
/// but keep the index).
fn tokenize(value: &str, delims: &str) -> Vec<String> {
    let chars: Vec<char> = value.chars().collect();
    let n = chars.len();
    let mut out: Vec<String> = Vec::new();
    let mut i = 0usize;
    if n > 0 && is_delim(chars[0], delims) {
        out.push(String::new()); // Java leading empty
        while i < n && is_delim(chars[i], delims) {
            i += 1;
        }
    }
    while i < n {
        let start = i;
        while i < n && !is_delim(chars[i], delims) {
            i += 1;
        }
        out.push(chars[start..i].iter().collect());
        while i < n && is_delim(chars[i], delims) {
            i += 1;
        }
    }
    out
}

/// Purely-numeric tokens collapse to `*` so IDs/counters do not fragment groups.
fn normalize_token(token: &str) -> String {
    if !token.is_empty() && token.chars().all(|c| c.is_ascii_digit()) {
        "*".to_string()
    } else {
        token.to_string()
    }
}

/// Build the TF (count-weighted) sparse vector for `value` under `mode`/`delims`,
/// keyed identically to the exact-mode `TextSimilarityClustering.vectorize`.
fn vectorize(value: &str, mode: MatchMode, delims: &str) -> HashMap<String, f64> {
    let mut m: HashMap<String, f64> = HashMap::new();
    if value.is_empty() {
        return m;
    }
    match mode {
        MatchMode::TermList => {
            for (idx, tok) in tokenize(value, delims).iter().enumerate() {
                if !tok.is_empty() {
                    let key = format!("{idx}-{}", normalize_token(tok));
                    *m.entry(key).or_insert(0.0) += 1.0;
                }
            }
        }
        MatchMode::TermSet => {
            for tok in tokenize(value, delims).iter() {
                if !tok.is_empty() {
                    *m.entry(normalize_token(tok)).or_insert(0.0) += 1.0;
                }
            }
        }
        MatchMode::NgramSet => {
            let chars: Vec<char> = value.chars().collect();
            if chars.len() < 3 {
                for c in &chars {
                    *m.entry(c.to_string()).or_insert(0.0) += 1.0;
                }
            } else {
                for w in chars.windows(3) {
                    let tri: String = w.iter().collect();
                    *m.entry(tri).or_insert(0.0) += 1.0;
                }
            }
        }
    }
    m
}

fn l2norm(m: &HashMap<String, f64>) -> f64 {
    m.values().map(|v| v * v).sum::<f64>().sqrt()
}

/// Sparse cosine between two TF maps with precomputed L2 norms (iterate the smaller).
fn cosine(a: &HashMap<String, f64>, a_norm: f64, b: &HashMap<String, f64>, b_norm: f64) -> f64 {
    if a_norm == 0.0 || b_norm == 0.0 {
        return 0.0;
    }
    let (small, large) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    let mut dot = 0.0;
    for (k, v) in small {
        if let Some(w) = large.get(k) {
            dot += v * w;
        }
    }
    dot / (a_norm * b_norm)
}

/// Similarity mirroring exact-mode `computeSimilarity`: both-empty = 1.0, one-empty
/// = 0.0, else sparse cosine.
fn similarity(
    doc: &HashMap<String, f64>,
    doc_norm: f64,
    doc_empty: bool,
    rep: &HashMap<String, f64>,
    rep_norm: f64,
    rep_empty: bool,
) -> f64 {
    if doc_empty && rep_empty {
        return 1.0;
    }
    if doc_empty || rep_empty {
        return 0.0;
    }
    cosine(doc, doc_norm, rep, rep_norm)
}

/// Argmax cosine over reps; returns the **1-based** best rep index when best sim
/// `>= t`, else `-1`. Ties resolve to the lowest index (first max wins via strict `>`).
#[allow(clippy::too_many_arguments)]
fn assign(
    doc: &HashMap<String, f64>,
    doc_norm: f64,
    doc_empty: bool,
    reps: &[HashMap<String, f64>],
    rep_norms: &[f64],
    rep_empty: &[bool],
    t: f64,
) -> i32 {
    let mut best = -1i32;
    let mut best_sim = f64::NEG_INFINITY;
    for (j, rep) in reps.iter().enumerate() {
        let sim = similarity(doc, doc_norm, doc_empty, rep, rep_norms[j], rep_empty[j]);
        if sim > best_sim {
            best_sim = sim;
            best = j as i32;
        }
    }
    if best >= 0 && best_sim >= t {
        best + 1 // 1-based label (LLD §4.4: matched labels are 1..K)
    } else {
        -1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;

    fn call_res(
        messages: Vec<Option<&str>>,
        reps_json: &str,
        t: f64,
        mode: &str,
        delims: &str,
    ) -> Result<Vec<i32>> {
        let udf = ClusterAssignUdf::new();
        let n = messages.len();
        let msg: ArrayRef = Arc::new(StringArray::from(messages));
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(msg),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(reps_json.to_string()))),
                ColumnarValue::Scalar(ScalarValue::Float64(Some(t))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(mode.to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(delims.to_string()))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("message", DataType::Utf8, true)),
                Arc::new(Field::new("reps", DataType::Utf8, false)),
                Arc::new(Field::new("t", DataType::Float64, false)),
                Arc::new(Field::new("match", DataType::Utf8, false)),
                Arc::new(Field::new("delims", DataType::Utf8, false)),
            ],
            number_rows: n,
            return_field: Arc::new(Field::new("label", DataType::Int32, true)),
            config_options: Arc::new(Default::default()),
        };
        match udf.invoke_with_args(args)? {
            ColumnarValue::Array(a) => Ok(a
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()),
            other => panic!("expected array, got {other:?}"),
        }
    }

    fn call(messages: Vec<Option<&str>>, reps_json: &str, t: f64, mode: &str, delims: &str) -> Vec<i32> {
        call_res(messages, reps_json, t, mode, delims).unwrap()
    }

    #[test]
    fn termset_argmax_and_threshold_1based() {
        // reps as event TEXTS; termset = bag of words.
        let reps = r#"["error timeout on host","user login ok"]"#;
        let out = call(
            vec![
                Some("error timeout host db01"),
                Some("user login ok"),
                Some("totally unrelated words"),
            ],
            reps,
            0.3,
            "termset",
            "non-alphanumeric",
        );
        assert_eq!(out[0], 1); // rep index 0 -> label 1
        assert_eq!(out[1], 2); // rep index 1 -> label 2
        assert_eq!(out[2], -1); // no shared terms
    }

    #[test]
    fn numeric_normalization_collapses_ids() {
        // Two docs with different numeric IDs must land on the same rep (digits -> "*").
        let reps = r#"["request id 100 done"]"#;
        let out = call(
            vec![Some("request id 4242 done"), Some("request id 9 done")],
            reps,
            0.9,
            "termset",
            "non-alphanumeric",
        );
        assert_eq!(out[0], 1);
        assert_eq!(out[1], 1);
    }

    #[test]
    fn termlist_is_positional() {
        // Same bag, different order: termlist positional keys differ -> no match; termset matches.
        let reps = r#"["alpha beta gamma"]"#;
        let shuffled = vec![Some("gamma beta alpha")];
        assert_eq!(call(shuffled.clone(), reps, 0.9, "termlist", "non-alphanumeric")[0], -1);
        assert_eq!(call(shuffled, reps, 0.9, "termset", "non-alphanumeric")[0], 1);
    }

    #[test]
    fn ngramset_trigrams() {
        let reps = r#"["database"]"#;
        // "databASE" vs "database": shares most trigrams -> matches at moderate t.
        let out = call(vec![Some("database")], reps, 0.9, "ngramset", "non-alphanumeric");
        assert_eq!(out[0], 1);
    }

    #[test]
    fn null_message_is_unmatched() {
        let reps = r#"["alpha"]"#;
        let out = call(vec![None, Some("alpha")], reps, 0.1, "termset", "non-alphanumeric");
        assert_eq!(out[0], -1); // null -> "" -> no non-empty rep matches
        assert_eq!(out[1], 1);
    }

    #[test]
    fn threshold_out_of_range_errors() {
        let reps = r#"["a"]"#;
        assert!(call_res(vec![Some("a")], reps, 1.0, "termset", "non-alphanumeric").is_err());
        assert!(call_res(vec![Some("a")], reps, 0.0, "termset", "non-alphanumeric").is_err());
    }

    #[test]
    fn too_many_reps_errors() {
        let reps: Vec<String> = (0..51).map(|i| format!("\"rep {i}\"")).collect();
        let reps_json = format!("[{}]", reps.join(","));
        assert!(call_res(vec![Some("rep 1")], &reps_json, 0.5, "termset", "non-alphanumeric").is_err());
    }
}
