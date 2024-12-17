//! Symsrv helpers.

use super::cache;
use anyhow::Context;
use symsrv::SymsrvDownloader;

pub struct Symsrv {
    downloader: SymsrvDownloader,
}

impl Symsrv {
    pub fn new(cache_dir: &std::path::Path, symbol_servers: &crate::config::SymbolServers) -> Self {
        let downloader = SymsrvDownloader::new(
            symbol_servers
                .windows
                .iter()
                .map(|url| symsrv::NtSymbolPathEntry::Chain {
                    dll: "symsrv.dll".into(),
                    cache_paths: vec![symsrv::CachePath::Path(cache_dir.join("windows"))],
                    urls: vec![url.clone()],
                })
                .collect(),
        );
        Symsrv { downloader }
    }

    pub async fn get_file(
        &self,
        key: &cache::Key,
        cache_entry: &cache::LiveEntry,
    ) -> anyhow::Result<std::path::PathBuf> {
        // SymsrvObserver doesn't have async methods, so we can't suspend a download from
        // occurring. There's no point in trying to use the detected `total_bytes`: we could do a
        // song and dance to extract that value concurrently and use it, but we'd only want to do
        // that to pause the download until the cache has enough space.
        cache_entry.reserve_space(super::UNKNOWN_SIZE).await;
        let path = self
            .downloader
            .get_file(&key.debug_name, &key.debug_id.breakpad().to_string())
            .await?;
        let len = tokio::fs::metadata(&path)
            .await
            .with_context(|| format!("failed to get metadata for {}", path.display()))?
            .len();
        cache_entry.force_reserve_space(len);
        Ok(path)
    }
}
