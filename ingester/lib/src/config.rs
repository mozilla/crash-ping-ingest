use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub redash: Redash,
    pub symbol_servers: SymbolServers,
    #[serde(default)]
    pub worker_threads: WorkerThreads,
    pub cache: Cache,
    pub signature: Signature,
}

#[derive(Debug, Deserialize)]
pub struct Cache {
    #[serde(default)]
    pub size_limit_gb: CacheSizeLimit,
    pub directory: std::path::PathBuf,
}

#[derive(Copy, Clone, Debug, Default, Deserialize)]
pub enum CacheSizeLimit {
    #[default]
    #[serde(rename = "none")]
    None,
    #[serde(untagged)]
    Limit(u64),
}

impl CacheSizeLimit {
    pub fn limit(self) -> Option<u64> {
        match self {
            Self::None => None,
            Self::Limit(n) => Some(n),
        }
    }

    pub fn limit_bytes(self) -> Option<u64> {
        self.limit().map(|n| n * 1_000_000_000)
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Signature {
    pub generator: String,
}

#[derive(Copy, Clone, Debug, Default, Deserialize)]
pub enum WorkerThreads {
    #[default]
    #[serde(rename = "auto")]
    Auto,
    #[serde(untagged)]
    Exact(usize),
}

fn default_concurrent_downloads() -> usize {
    4
}

#[derive(Clone, Debug, Deserialize)]
pub struct SymbolServers {
    #[serde(default)]
    pub breakpad: Vec<String>,
    #[serde(default)]
    pub windows: Vec<String>,
    #[serde(default = "default_concurrent_downloads")]
    pub concurrency: usize,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Stringish {
    Bool(bool),
    Int(isize),
    Float(f64),
    String(String),
}

impl Stringish {
    fn into_string(self) -> String {
        match self {
            Stringish::Bool(b) => b.to_string(),
            Stringish::Int(i) => i.to_string(),
            Stringish::Float(f) => f.to_string(),
            Stringish::String(s) => s,
        }
    }

    fn map<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<String, String>, D::Error> {
        let s = <BTreeMap<String, Stringish>>::deserialize(deserializer)?;
        Ok(s.into_iter().map(|(k, v)| (k, v.into_string())).collect())
    }
}

#[derive(Clone, Default, Debug, serde::Deserialize)]
struct RedashParams(#[serde(deserialize_with = "Stringish::map")] BTreeMap<String, String>);

impl RedashParams {
    fn merge_new(mut self, other: &Self) -> Self {
        for (k, v) in &other.0 {
            if !self.0.contains_key(k) {
                self.0.insert(k.clone(), v.clone());
            }
        }
        self
    }
}

/// Parameters precedence is `parameters` < `matrix` < `queries`.
#[derive(Debug, serde::Deserialize)]
pub struct Redash {
    pub query_id: usize,
    pub api_key: String,
    pub max_age_seconds: Option<usize>,
    #[serde(default)]
    parameters: RedashParams,
    #[serde(default)]
    queries: Vec<RedashParams>,
    #[serde(default)]
    matrix: BTreeMap<String, BTreeMap<String, RedashParams>>,
}

impl Redash {
    /// Generate all configured query parameter sets.
    pub fn query_parameter_sets(&self) -> impl Iterator<Item = BTreeMap<String, String>> + '_ {
        self.matrix
            .values()
            .fold(self.queries.clone(), |queries, v| {
                v.values()
                    .flat_map(|params| queries.clone().into_iter().map(|q| q.merge_new(params)))
                    .collect()
            })
            .into_iter()
            .map(|q| q.merge_new(&self.parameters).0)
    }
}
