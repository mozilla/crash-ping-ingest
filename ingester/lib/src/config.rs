use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
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
