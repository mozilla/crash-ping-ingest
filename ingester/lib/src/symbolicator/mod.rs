use super::Config;
use crate::config::SymbolServers;
use anyhow::Context;
use debugid::DebugId;
use futures_util::{future::FutureExt, stream::StreamExt};
use samply_symbols::{
    BreakpadIndexParser, FileAndPathHelper, FileAndPathHelperResult, FileLocation,
    FramesLookupResult, LibraryInfo, LookupAddress, SymbolManager, SyncAddressInfo,
};
use std::{collections::HashMap, mem::ManuallyDrop, num::ParseIntError, sync::Arc};
use tokio::sync::Semaphore;

mod cache;
mod symbol_maps;
mod symsrv;

// Limits to the top and bottom frames, relative to a traceback ("top" being the innermost frame).
// The rest of the frames are omitted. MAX_TOP_FRAMES should be at least 10 for siggen to work
// properly.
const MAX_TOP_FRAMES: usize = 50;
const MAX_BOTTOM_FRAMES: usize = 50;
const MAX_FRAMES: usize = MAX_TOP_FRAMES + MAX_BOTTOM_FRAMES;

/// Spawn an asynchronous file download on a dedicated thread, so that it is not starved (and won't
/// timeout) when many other tasks are running. Since we potentially spawn tens of thousands of
/// tasks, even if we constrain how many are running there still may be enough thrashing to delay
/// things.
///
/// We do this roundabout approach rather than using a full blocking API because some functions are
/// only async (like those in symsrv) and it's a bit cleaner and more semantically friendly to
/// bracket the necessary code with a function.
// Not currently used.
#[allow(unused)]
async fn file_download<'a, F>(future: F) -> F::Output
where
    F: std::future::Future + Send + 'a,
    F::Output: Send + 'a,
{
    let future: futures_util::future::BoxFuture<'a, Box<()>> = Box::pin(async move {
        let ret: Box<F::Output> = Box::new(future.await);
        unsafe { std::mem::transmute::<_, Box<()>>(ret) }
    });
    let future = unsafe {
        std::mem::transmute::<_, futures_util::future::BoxFuture<'static, Box<()>>>(future)
    };
    let ret =
        tokio::task::spawn_blocking(move || tokio::runtime::Handle::current().block_on(future))
            .await
            .unwrap();
    *unsafe { std::mem::transmute::<_, Box<F::Output>>(ret) }
}

pub struct Symbolicator {
    symbol_manager: Arc<SymbolManager<FileHelper>>,
    symbol_maps: symbol_maps::SymbolMaps,
    cache: cache::Cache,
    only_crashing_thread: bool,
}

impl Symbolicator {
    pub fn new(
        config: &Config,
        status: &Arc<crate::Status>,
        only_crashing_thread: bool,
    ) -> anyhow::Result<Self> {
        let (helper, cache) = FileHelper::new(config)?;
        cache.update_status(status);
        Ok(Symbolicator {
            symbol_manager: Arc::new(SymbolManager::with_helper(helper)),
            symbol_maps: Default::default(),
            cache,
            only_crashing_thread,
        })
    }

    pub fn symbolicate(
        &self,
        stack_traces_json: String,
        channel: String,
    ) -> impl std::future::Future<Output = anyhow::Result<Symbolicated>> {
        let symbol_map_getter = self.symbol_maps.getter(
            self.symbol_manager.clone(),
            &self.cache,
            cache::Channel::from(channel.as_str()),
        );

        // Spawn a task to ensure that all calls to symbolicate are executed concurrently, which
        // avoids potential deadlock if the cache limit is reached (sequencing would introduce
        // unnecessary execution dependencies).
        let only_crashing_thread = self.only_crashing_thread;
        tokio::spawn(async move {
            let stack_traces = serde_json::from_str::<json::StackTraces>(&stack_traces_json)?;

            fn thread_frames(t: json::Thread<'_>) -> Result<Vec<Frame>, ParseIntError> {
                t.frames.into_iter().map(Frame::try_from).collect()
            }

            let mut threads = if only_crashing_thread {
                if let Some(ind) = stack_traces.crash_thread {
                    stack_traces
                        .threads
                        .into_iter()
                        .nth(ind)
                        .map(thread_frames)
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()?
                } else {
                    Default::default()
                }
            } else {
                stack_traces
                    .threads
                    .into_iter()
                    .map(thread_frames)
                    .collect::<Result<Vec<_>, _>>()?
            };

            // Limit thread frames to MAX_FRAMES.
            for t in &mut threads {
                if t.len() > MAX_FRAMES {
                    let to_omit = t.len() - MAX_FRAMES;
                    t.splice(
                        MAX_TOP_FRAMES..(MAX_TOP_FRAMES + to_omit),
                        [Frame {
                            ip: 0,
                            module_index: None,
                            info: vec![FrameInfo {
                                omitted: Some(to_omit),
                                ..Default::default()
                            }],
                        }],
                    );
                }
            }

            let used_modules: std::collections::HashSet<usize> = threads
                .iter()
                .flat_map(|t| t)
                .filter_map(|f| f.module_index)
                .collect();

            let modules: Vec<(usize, Module)> = used_modules
                .into_iter()
                .filter_map(|i| match Module::try_from(stack_traces.modules.get(i)?) {
                    Err(e) => {
                        log::debug!("failed to get module: {e:#}");
                        None
                    }
                    Ok(module) => Some((i, module)),
                })
                .collect();

            // This creates a FuturesUnordered for resolving modules. As they are available, we'll
            // fill out all the relevant frames. It's important to poll them all concurrently to
            // avoid dependency deadlock (accidentally imposing some ordering of module
            // availability).
            let mut modules = symbol_map_getter.get_symbol_maps(
                modules
                    .into_iter()
                    .map(|(index, module)| {
                        let key = cache::Key {
                            debug_name: module.debug_file.clone(),
                            debug_id: module.debug_id.clone(),
                        };
                        ((index, module), key)
                    })
                    .collect(),
            );

            while let Some(((index, module), map)) = modules.next().await {
                let Some(map) = map else {
                    continue;
                };
                for ping_frame in threads
                    .iter_mut()
                    .flat_map(|t| t)
                    .filter(|f| f.module_index == Some(index))
                {
                    let offset: u32 = (ping_frame.ip - module.base_address).try_into().unwrap();
                    if let Some(SyncAddressInfo {
                        frames: Some(FramesLookupResult::Available(frames)),
                        symbol,
                    }) = map.lookup_sync(LookupAddress::Relative(offset))
                    {
                        ping_frame.info = frames
                            .into_iter()
                            .map(|f| FrameInfo {
                                file: f.file_path.map(|p| match p.mapped_path() {
                                    Some(mapped) => mapped.to_special_path_str(),
                                    None => p.raw_path().to_string(),
                                }),
                                line: f.line_number,
                                module: module
                                    .filename
                                    .as_ref()
                                    .or(Some(&module.debug_file))
                                    .cloned(),
                                module_offset: Some(format!("{offset:#018x}")),
                                function: f.function,
                                function_offset: Some(format!("{:#018x}", offset - symbol.address)),
                                offset: Some(format!("{:#018x}", ping_frame.ip)),
                                omitted: None,
                            })
                            .collect();
                    }
                }
            }

            // Combine symbolicated thread frames.
            let threads = threads
                .into_iter()
                .map(|t| {
                    let mut frames: Vec<FrameInfo> = t
                        .into_iter()
                        .flat_map(|f| f.into_frame_info(&stack_traces.modules))
                        .collect();

                    // Limit combined frames to MAX_FRAMES again. Symbolication may have introduced
                    // more inlined frames after the first pass. We do the first pass as an optimization to
                    // avoid unnecessary frame lookups.
                    if frames.len() > MAX_FRAMES {
                        let to_omit = frames.len() - MAX_FRAMES;
                        // If there is already an `omitted` entry, it is guaranteed to be in the
                        // discarded range, because `into_frame_info` always produces at least one
                        // value and we are using the same top/bottom limits (i.e., the original
                        // `MAX_TOP_FRAMES` will expand to at least `MAX_TOP_FRAMES` here which
                        // will not have an omitted value, and likewise for the
                        // `MAX_BOTTOM_FRAMES`.
                        let total_omitted: usize = frames
                            [MAX_TOP_FRAMES..(MAX_TOP_FRAMES + to_omit)]
                            .iter()
                            .map(|info| info.omitted.unwrap_or(1))
                            .sum();
                        frames.splice(
                            MAX_TOP_FRAMES..(MAX_TOP_FRAMES + to_omit),
                            [FrameInfo {
                                omitted: Some(total_omitted),
                                ..Default::default()
                            }],
                        );
                    }

                    ThreadInfo { frames }
                })
                .collect();

            Ok(Symbolicated {
                reason: stack_traces.crash_type.map(|s| s.into_owned()),
                crashing_thread: stack_traces.crash_thread,
                threads,
            })
        })
        .map(|r| r.map_err(anyhow::Error::from).and_then(|r| r))
    }

    pub async fn finish_tasks(self) {
        let Symbolicator {
            symbol_manager: manager,
            cache,
            symbol_maps,
            only_crashing_thread: _,
        } = self;

        // NOTE: binding `symbol_maps: _` or using `..` above will still keep the symbol maps in
        // scope until the function exits! Thus, we explicitly bind and drop it (as it holds the
        // LiveEntry values and must be dropped for eviction to work).
        drop(symbol_maps);

        log::debug!("entering eviction loop");
        if let Some(evictor) = cache.end_jobs() {
            loop {
                if !evictor.evict(|key| manager.helper().evict(&key)).await {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        } else {
            futures_util::future::pending().await
        }
    }
}

struct Module {
    base_address: usize,
    filename: Option<String>,
    debug_file: String,
    debug_id: DebugId,
}

struct Frame {
    ip: usize,
    module_index: Option<usize>,
    info: Vec<FrameInfo>,
}

impl Frame {
    /// Return the frame info, or create a frame info with the minimal information available if no
    /// info was populated.
    ///
    /// This is guaranteed to return a non-empty Vec.
    pub fn into_frame_info(self, modules: &[json::Module]) -> Vec<FrameInfo> {
        if self.info.is_empty() {
            let mut module = None;
            let mut module_offset = None;
            if let Some(m) = self.module_index.and_then(|i| modules.get(i)) {
                module = m
                    .filename
                    .as_ref()
                    .or(m.debug_file.as_ref())
                    .map(|s| s.to_string());
                if let Some(base) = m.base_address.as_ref().and_then(|b| parse_hex(&b).ok()) {
                    module_offset = Some(format!("{:#018x}", self.ip - base));
                }
            }
            vec![FrameInfo {
                module,
                module_offset,
                offset: Some(format!("{:#018x}", self.ip)),
                ..Default::default()
            }]
        } else {
            self.info
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct Symbolicated {
    pub reason: Option<String>,
    pub crashing_thread: Option<usize>,
    pub threads: Vec<ThreadInfo>,
}

#[derive(Debug, serde::Serialize)]
pub struct ThreadInfo {
    pub frames: Vec<FrameInfo>,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct FrameInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function_offset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub module: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub module_offset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub omitted: Option<usize>,
}

fn parse_hex(s: &str) -> Result<usize, ParseIntError> {
    usize::from_str_radix(s.trim_start_matches("0x"), 16)
}

impl TryFrom<json::Frame<'_>> for Frame {
    type Error = ParseIntError;

    fn try_from(value: json::Frame) -> Result<Self, Self::Error> {
        Ok(Frame {
            ip: parse_hex(&value.ip)?,
            module_index: value.module_index,
            info: Default::default(),
        })
    }
}

impl TryFrom<&json::Module<'_>> for Module {
    type Error = anyhow::Error;

    fn try_from(value: &json::Module<'_>) -> Result<Self, Self::Error> {
        Ok(Module {
            base_address: parse_hex(
                value
                    .base_address
                    .as_ref()
                    .context("missing base_address")?,
            )?,
            filename: value.filename.as_ref().map(|s| s.to_string()),
            // Fix the debug file entries of modules to only be the file basenames. This is fixed
            // by bug 1931237 but it will take a while to get into release.
            // `unwrap` will never fail because `rsplit` is guaranteed to return at least one
            // value.
            debug_file: value
                .debug_file
                .as_ref()
                .context("missing debug_file")?
                .rsplit("/")
                .next()
                .unwrap()
                .into(),
            debug_id: value
                .debug_id
                .as_ref()
                .context("missing debug_id")?
                .parse()?,
        })
    }
}

#[derive(Clone)]
struct Location {
    key: cache::Key,
    symindex: bool,
    cache_entry: cache::LiveEntry,
}

impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.key.fmt(f)?;
        if self.symindex {
            write!(f, ".symindex")?;
        }
        Ok(())
    }
}

impl cache::Key {
    fn breakpad_relative_path(&self) -> String {
        format!(
            "{}/{}/{}.sym",
            self.debug_name,
            self.debug_id.breakpad(),
            self.debug_name.trim_end_matches(".pdb"),
        )
    }

    fn breakpad_symindex_relative_path(&self) -> String {
        format!(
            "{}/{}/{}.symindex",
            self.debug_name,
            self.debug_id.breakpad(),
            self.debug_name.trim_end_matches(".pdb"),
        )
    }

    fn symsrv_relative_path(&self) -> String {
        format!(
            "{}/{}/{}",
            self.debug_name,
            self.debug_id.breakpad(),
            self.debug_name,
        )
    }
}

impl FileLocation for Location {
    fn location_for_dyld_subcache(&self, _suffix: &str) -> Option<Self> {
        None
    }

    fn location_for_external_object_file(&self, _object_file: &str) -> Option<Self> {
        None
    }

    fn location_for_pdb_from_binary(&self, _pdb_path_in_binary: &str) -> Option<Self> {
        None
    }

    fn location_for_source_file(&self, _source_file_path: &str) -> Option<Self> {
        None
    }

    fn location_for_breakpad_symindex(&self) -> Option<Self> {
        let mut loc = self.clone();
        loc.symindex = true;
        Some(loc)
    }

    fn location_for_dwo(&self, _comp_dir: &str, _path: &str) -> Option<Self> {
        None
    }

    fn location_for_dwp(&self) -> Option<Self> {
        None
    }
}

struct FileHelper {
    cache_dir: std::path::PathBuf,
    symbol_servers: SymbolServers,
    downloads: Semaphore,
    client: reqwest::Client,
    symsrv: symsrv::Symsrv,
    loaded_files: std::sync::Mutex<HashMap<cache::Key, Loaded>>,
}

struct Loaded {
    mapped: Option<Arc<memmap2::Mmap>>,
    breakpad_symindex_mapped: Option<Arc<memmap2::Mmap>>,
}

const AMAZON_ORIGIN_LENGTH: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("x-amz-meta-original_size");

const GOOGLE_CONTENT_LENGTH: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("x-goog-stored-content-length");

const UNKNOWN_SIZE: u64 = 50_000_000;

impl FileHelper {
    fn new(config: &Config) -> anyhow::Result<(Self, cache::Cache)> {
        let cache_dir = config.cache.directory.clone();
        let symbol_servers = config.symbol_servers.clone();
        let downloads = Semaphore::new(config.symbol_servers.concurrency);
        let client = reqwest::Client::builder()
            .user_agent(super::APP_USER_AGENT)
            .build()?;
        let symsrv = symsrv::Symsrv::new(&cache_dir, &config.symbol_servers);
        let cache = cache::Cache::new(
            config.cache.size_limit_gb.limit_bytes(),
            cache_dir.join("state"),
        );
        let keys = cache.live_keys();

        Ok((
            FileHelper {
                cache_dir,
                symbol_servers,
                downloads,
                client,
                symsrv,
                loaded_files: std::sync::Mutex::new(
                    keys.into_iter()
                        .map(|key| {
                            (
                                key,
                                Loaded {
                                    mapped: None,
                                    breakpad_symindex_mapped: None,
                                },
                            )
                        })
                        .collect(),
                ),
            },
            cache,
        ))
    }

    fn evict(&self, key: &cache::Key) {
        let entry = self.loaded_files.lock().unwrap().remove(key);
        if let Some(loaded) = entry {
            drop(loaded);
            let paths = [
                self.cache_dir
                    .join("breakpad")
                    .join(key.breakpad_relative_path()),
                self.cache_dir
                    .join("breakpad")
                    .join(key.breakpad_symindex_relative_path()),
                self.cache_dir
                    .join("windows")
                    .join(key.symsrv_relative_path()),
            ];
            for path in paths {
                let _ = std::fs::remove_file(path);
            }
        }
    }

    fn get_mapped(&self, key: &cache::Key) -> Option<Arc<memmap2::Mmap>> {
        let mut guard = self.loaded_files.lock().unwrap();
        let loaded = guard.get_mut(key)?;
        if loaded.mapped.is_none() {
            let f = std::fs::File::open(
                self.cache_dir
                    .join("breakpad")
                    .join(key.breakpad_relative_path()),
            )
            .ok()
            .or_else(|| {
                std::fs::File::open(
                    self.cache_dir
                        .join("windows")
                        .join(key.symsrv_relative_path()),
                )
                .ok()
            })?;
            loaded.mapped = Some(Arc::new(unsafe { memmap2::Mmap::map(&f) }.ok()?));
        }
        loaded.mapped.clone()
    }

    fn get_breakpad_symindex_mapped(&self, key: &cache::Key) -> Option<Arc<memmap2::Mmap>> {
        let mut guard = self.loaded_files.lock().unwrap();
        let loaded = guard.get_mut(key)?;
        if loaded.breakpad_symindex_mapped.is_none() {
            let f = std::fs::File::open(
                self.cache_dir
                    .join("breakpad")
                    .join(key.breakpad_symindex_relative_path()),
            )
            .ok()?;
            loaded.breakpad_symindex_mapped =
                Some(Arc::new(unsafe { memmap2::Mmap::map(&f) }.ok()?));
        }
        loaded.breakpad_symindex_mapped.clone()
    }

    async fn load_breakpad_file(
        &self,
        key: &cache::Key,
        cache_entry: &cache::LiveEntry,
    ) -> anyhow::Result<Loaded> {
        let rel_path = key.breakpad_relative_path();
        let symindex_rel_path = key.breakpad_symindex_relative_path();

        for server in &self.symbol_servers.breakpad {
            match self
                .load_breakpad_file_from_server(server, &rel_path, &symindex_rel_path, cache_entry)
                .await
            {
                Err(e) => {
                    log::info!("failed to get breakpad sym file for {key} from {server}: {e:#}")
                }
                Ok(v) => return Ok(v),
            }
        }

        anyhow::bail!("couldn't find breakpad symbol file for {key}")
    }

    async fn load_symsrv_file(
        &self,
        key: &cache::Key,
        cache_entry: &cache::LiveEntry,
    ) -> anyhow::Result<Loaded> {
        let path = self.symsrv.get_file(key, cache_entry).await?;
        let f = tokio::fs::File::open(path).await?;
        let mapped = Arc::new(unsafe { memmap2::Mmap::map(&f) }?);

        Ok(Loaded {
            mapped: Some(mapped),
            breakpad_symindex_mapped: None,
        })
    }

    async fn get_download_size(&self, url: &str) -> anyhow::Result<Option<u64>> {
        let mut response = self.client.head(url).send().await?.error_for_status()?;
        // Some servers, like the mozilla symbol server, respond with 200 to indicate an entry
        // exists, _without_ redirecting. They still provide the `Location` header, and we want to
        // inspect the final location to get the size, so manually follow the redirect.
        while let Some(location) = response.headers().get(reqwest::header::LOCATION) {
            let Ok(location_str) = location.to_str() else {
                // Just give up and hope that the GET will succeed (though this is otherwise an
                // indication of an issue that will likely affect GET).
                return Ok(None);
            };

            response = self
                .client
                .head(location_str)
                .send()
                .await?
                .error_for_status()?;
        }

        fn int_header<K: reqwest::header::AsHeaderName>(
            response: &reqwest::Response,
            name: K,
        ) -> Option<u64> {
            response
                .headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok())
        }

        let size_headers = [
            (!response
                .headers()
                .contains_key(reqwest::header::CONTENT_ENCODING))
            .then_some(reqwest::header::CONTENT_LENGTH),
            Some(AMAZON_ORIGIN_LENGTH),
            Some(GOOGLE_CONTENT_LENGTH),
        ];

        Ok(size_headers
            .into_iter()
            .find_map(|h| h.and_then(|h| int_header(&response, h))))
    }

    async fn load_breakpad_file_from_server(
        &self,
        server: &str,
        rel_path: &str,
        symindex_rel_path: &str,
        cache_entry: &cache::LiveEntry,
    ) -> anyhow::Result<Loaded> {
        let url = format!("{server}/{rel_path}");

        // Reserve space in the cache. This doesn't attempt to account for the symindex file size,
        // though perhaps in the future it could make a guess.
        let size = self.get_download_size(&url).await?.unwrap_or(UNKNOWN_SIZE);
        cache_entry.reserve_space(size).await;

        let mut response = self.client.get(&url).send().await?.error_for_status()?;

        let path = self.cache_dir.join("breakpad").join(rel_path);
        std::fs::create_dir_all(path.parent().unwrap())?;
        let mut f = TempFile::create_new(&path)
            .await
            .with_context(|| format!("failed to create file at {}", path.display()))?;
        let mut index = BreakpadIndexParser::new();

        loop {
            match response.chunk().await {
                Err(e) => anyhow::bail!("error getting response: {e}"),
                Ok(Some(chunk)) => {
                    use tokio::io::AsyncWriteExt;
                    f.write_all(&chunk)
                        .await
                        .context("failed to write chunk to file")?;
                    index.consume(&chunk);
                }
                Ok(None) => break,
            }
        }

        async fn try_write_index<'a>(
            path: &'a std::path::Path,
            parser: BreakpadIndexParser,
        ) -> anyhow::Result<TempFile<'a>> {
            use tokio::io::AsyncWriteExt;
            let index = parser.finish()?;
            let mut f = TempFile::create_new(path).await?;
            f.write_all(&index.serialize_to_bytes()).await?;
            Ok(f)
        }

        let symindex_path = self.cache_dir.join("breakpad").join(symindex_rel_path);
        let index_file = match try_write_index(&symindex_path, index).await {
            Ok(v) => Some(v),
            Err(e) => {
                log::error!("failed to generate breakpad symindex file: {e:#}");
                None
            }
        };

        // Update stored size
        {
            let mut size = f
                .metadata()
                .await
                .context("failed to get file metadata")?
                .len();
            if let Some(index_file) = &index_file {
                if let Some(len) = index_file.metadata().await.ok().map(|f| f.len()) {
                    size += len;
                }
            }
            cache_entry.exists_with_space(size);
        }

        let mapped = Arc::new(unsafe { memmap2::Mmap::map(&*f) }.context("failed to map file")?);

        TempFile::retain(f);
        if let Some(f) = index_file {
            TempFile::retain(f);
        }

        Ok(Loaded {
            mapped: Some(mapped),
            // Only map the symindex file upon request.
            breakpad_symindex_mapped: None,
        })
    }
}

struct TempFile<'a> {
    path: &'a std::path::Path,
    file: ManuallyDrop<tokio::fs::File>,
}

impl std::ops::Deref for TempFile<'_> {
    type Target = tokio::fs::File;
    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl std::ops::DerefMut for TempFile<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.file
    }
}

impl<'a> TempFile<'a> {
    async fn create_new<P: AsRef<std::path::Path> + ?Sized>(path: &'a P) -> std::io::Result<Self> {
        Ok(TempFile {
            path: path.as_ref(),
            file: ManuallyDrop::new(tokio::fs::File::create_new(path).await?),
        })
    }

    fn retain(mut self: Self) -> tokio::fs::File {
        let file = unsafe { ManuallyDrop::take(&mut self.file) };
        std::mem::forget(self);
        file
    }
}

impl Drop for TempFile<'_> {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.path);
    }
}

struct Contents(Arc<memmap2::Mmap>);

impl std::ops::Deref for Contents {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FileAndPathHelper for FileHelper {
    type F = Contents;
    type FL = Location;

    fn get_candidate_paths_for_debug_file(
        &self,
        _info: &LibraryInfo,
    ) -> FileAndPathHelperResult<Vec<samply_symbols::CandidatePathInfo<Self::FL>>> {
        Ok(vec![])
    }

    fn get_candidate_paths_for_binary(
        &self,
        _info: &LibraryInfo,
    ) -> FileAndPathHelperResult<Vec<samply_symbols::CandidatePathInfo<Self::FL>>> {
        Ok(vec![])
    }

    fn get_dyld_shared_cache_paths(
        &self,
        _arch: Option<&str>,
    ) -> FileAndPathHelperResult<Vec<Self::FL>> {
        Ok(vec![])
    }

    fn load_file(
        &self,
        location: Self::FL,
    ) -> std::pin::Pin<
        Box<
            dyn samply_symbols::OptionallySendFuture<Output = FileAndPathHelperResult<Self::F>>
                + '_,
        >,
    > {
        Box::pin(async move {
            if !self
                .loaded_files
                .lock()
                .unwrap()
                .contains_key(&location.key)
            {
                let loaded = 'result: {
                    let _permit = self.downloads.acquire().await;
                    match self
                        .load_breakpad_file(&location.key, &location.cache_entry)
                        .await
                    {
                        Ok(loaded) => break 'result loaded,
                        Err(e) => log::info!("failed to get breakpad file for {location}: {e:#}"),
                    }
                    match self
                        .load_symsrv_file(&location.key, &location.cache_entry)
                        .await
                    {
                        Ok(loaded) => break 'result loaded,
                        Err(e) => log::info!("failed to get symsrv file for {location}: {e:#}"),
                    }
                    return Err(format!(
                        "could not find debug file in symbol servers for {location}"
                    )
                    .into());
                };
                let prev = self
                    .loaded_files
                    .lock()
                    .unwrap()
                    .insert(location.key.clone(), loaded);
                assert!(
                    prev.is_none(),
                    "load_file should only be called with a particular location once"
                );
            }

            let mapped = if !location.symindex {
                self.get_mapped(&location.key)
            } else {
                self.get_breakpad_symindex_mapped(&location.key)
            };

            if let Some(mapped) = mapped {
                Ok(Contents(mapped))
            } else {
                Err(format!("failed to load mapped file for {location}").into())
            }
        })
    }
}

mod json {
    use serde::Deserialize;
    use std::borrow::Cow;

    // Android stack traces are camelCase rather than snake_case (bug 1931891 should fix this), so
    // we use aliases where necessary.

    #[derive(Debug, Deserialize)]
    pub struct StackTraces<'a> {
        #[serde(alias = "crashThread")]
        pub crash_thread: Option<usize>,
        #[serde(alias = "crashType")]
        pub crash_type: Option<Cow<'a, str>>,
        #[serde(borrow, default)]
        pub modules: Vec<Module<'a>>,
        #[serde(default)]
        pub threads: Vec<Thread<'a>>,
    }

    #[derive(Debug, Deserialize)]
    pub struct Module<'a> {
        #[serde(alias = "baseAddress")]
        pub base_address: Option<Cow<'a, str>>,
        #[serde(alias = "endAddress")]
        #[allow(unused)]
        pub end_address: Option<Cow<'a, str>>,
        pub filename: Option<Cow<'a, str>>,
        #[serde(alias = "codeId")]
        #[allow(unused)]
        pub code_id: Option<Cow<'a, str>>,
        #[serde(alias = "debugFile")]
        pub debug_file: Option<Cow<'a, str>>,
        #[serde(alias = "debugId")]
        pub debug_id: Option<Cow<'a, str>>,
        #[allow(unused)]
        pub version: Option<Cow<'a, str>>,
    }

    #[derive(Debug, Deserialize)]
    pub struct Thread<'a> {
        #[serde(borrow, default)]
        pub frames: Vec<Frame<'a>>,
    }

    #[derive(Debug, Deserialize)]
    pub struct Frame<'a> {
        pub ip: Cow<'a, str>,
        #[serde(alias = "moduleIndex")]
        pub module_index: Option<usize>,
        #[allow(unused)]
        pub trust: Cow<'a, str>,
    }
}
