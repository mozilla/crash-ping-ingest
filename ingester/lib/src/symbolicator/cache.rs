//! This cache API implements a specific flow where the cache waits until it has total
//! information about batch work before evicting anything. This allows the cache to retain files
//! until all jobs which may need them either use them, or are dropped.
//!
//! In particular, the API here enforces the following pattern:
//! 1. Load _all_ jobs and register what files they may use. Once each job is done registering,
//!    they can further interact with the cache when fetching files by reserving space.
//! 2. Once all jobs are loaded, evictions may begin (when possible).
//!
//! This interface does _not_ deal with the cached files themselves: it is only responsible for the
//! logical state of the cache.

use debugid::DebugId;
use std::collections::{BinaryHeap, HashMap};
use std::mem::ManuallyDrop;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed},
    Arc, Mutex,
};
use tokio::sync::Notify;

pub struct Cache {
    state: Arc<State>,
    live_entries: Arc<Mutex<HashMap<Key, LiveEntry>>>,
}

#[derive(Clone)]
pub struct CacheJob {
    state: Arc<State>,
    live_entries: Arc<Mutex<HashMap<Key, LiveEntry>>>,
}

impl Cache {
    pub fn new(limit: Option<u64>, persistent_storage: std::path::PathBuf) -> Self {
        let persisted: PersistedState = std::fs::File::open(&persistent_storage)
            .ok()
            .and_then(|r| serde_json::from_reader(r).ok())
            .unwrap_or_default();
        let state = Arc::new(State {
            inactive: Default::default(),
            inactive_change: Default::default(),
            limit: limit.map(CacheLimit::new),
            persist: persistent_storage,
        });
        let mut live_entries: HashMap<Key, LiveEntry> = Default::default();
        for info in persisted {
            let live = state.active(info.key.clone(), info.channel);
            live.exists_with_space(info.size.load(Relaxed));
            live_entries.insert(info.key.clone(), live);
        }

        Cache {
            state,
            live_entries: Arc::new(Mutex::new(live_entries)),
        }
    }

    pub fn update_status(&self, status: &Arc<crate::Status>) {
        if let Some(cache) = &status.cache {
            let weak_state = Arc::downgrade(&self.state);
            cache.set_current_callback(move || {
                weak_state
                    .upgrade()
                    .and_then(|state| {
                        state
                            .limit
                            .as_ref()
                            .map(|limit| limit.current.load(Relaxed))
                    })
                    .unwrap_or_default()
            });
        }
    }

    pub fn live_keys(&self) -> Vec<Key> {
        self.live_entries.lock().unwrap().keys().cloned().collect()
    }

    pub fn job(&self) -> CacheJob {
        CacheJob {
            state: self.state.clone(),
            live_entries: self.live_entries.clone(),
        }
    }

    pub fn end_jobs(self) -> Option<CacheEvictor> {
        if self.state.limit.is_some() {
            Some(CacheEvictor { state: self.state })
        } else {
            None
        }
    }
}

pub struct CacheEvictor {
    state: Arc<State>,
}

impl CacheEvictor {
    pub async fn evict<F: FnOnce(Key)>(&self, remove: F) -> bool {
        let limit = self.state.limit.as_ref().unwrap();
        let waiters_changed = limit.waiting_for_space.change();
        if !limit.waiting_for_space.has_waiters() {
            return false;
        }

        tokio::select! {
            _ = waiters_changed => true,
            _ = self.state.evict(remove) => true,
        }
    }
}

impl CacheJob {
    pub fn register(&self, file: Key, channel: Channel) -> LiveEntry {
        let entry = {
            let mut guard = self.live_entries.lock().unwrap();
            if !guard.contains_key(&file) {
                guard.insert(file.clone(), self.state.active(file.clone(), channel));
            }
            guard.get(&file).unwrap().clone()
        };
        entry.set_used();
        entry
    }
}

struct CacheLimit {
    limit: u64,
    current: AtomicU64,
    waiting_for_space: WaitingForSpace,
}

#[derive(Default)]
struct WaitingForSpace {
    waiters: AtomicUsize,
    notify: Notify,
}

struct WaitingForSpaceWaiter<'a> {
    inner: &'a WaitingForSpace,
}

impl<'a> WaitingForSpaceWaiter<'a> {
    fn new(inner: &'a WaitingForSpace) -> Self {
        inner.waiters.fetch_add(1, Relaxed);
        WaitingForSpaceWaiter { inner }
    }

    async fn wait(&self) {
        self.inner.notify.notified().await;
    }
}

impl<'a> Drop for WaitingForSpaceWaiter<'a> {
    fn drop(&mut self) {
        self.inner.waiters.fetch_sub(1, Relaxed);
    }
}

impl WaitingForSpace {
    fn waiter(&self) -> WaitingForSpaceWaiter {
        WaitingForSpaceWaiter::new(self)
    }

    async fn change(&self) {
        self.notify.notified().await;
    }

    fn notify(&self) {
        self.notify.notify_waiters();
    }

    fn has_waiters(&self) -> bool {
        self.waiters.load(Relaxed) > 0
    }
}

impl CacheLimit {
    fn new(limit: u64) -> Self {
        CacheLimit {
            limit,
            current: 0.into(),
            waiting_for_space: Default::default(),
        }
    }

    async fn wait_for_space(&self, amount: u64) {
        let check_amount = std::cmp::min(amount, self.limit);
        let mut waiter = None;
        while self
            .current
            .fetch_update(Relaxed, Relaxed, |v| {
                (v + check_amount <= self.limit).then_some(v + amount)
            })
            .is_err()
        {
            waiter
                .get_or_insert_with(|| self.waiting_for_space.waiter())
                .wait()
                .await;
        }
    }

    fn take_space(&self, amount: u64) {
        self.current.fetch_add(amount, Relaxed);
    }

    fn return_space(&self, amount: u64) {
        self.current.fetch_sub(amount, Relaxed);
        self.waiting_for_space.notify();
    }
}

/// A token which is held to indicate that an entry may be used.
///
/// When all tokens are dropped, the cache entry may be evicted.
#[derive(Clone)]
pub struct LiveEntry {
    inner: Arc<LiveEntryInner>,
}

impl LiveEntry {
    /// Reserve space in the cache for this entry. If there is not enough room, the future will
    /// suspend.
    pub async fn reserve_space(&self, size: u64) {
        if let Some(limit) = &self.inner.state.limit {
            let current = self.inner.info.size.load(Relaxed);
            if size < current {
                limit.return_space(current - size);
            } else if size > current {
                limit.wait_for_space(size - current).await;
            }
        }
        self.inner.info.size.store(size, Relaxed);
    }

    /// Reserve space in the cache for this entry, regardless of the cache size limit (useful for
    /// files that already exist).
    pub fn exists_with_space(&self, size: u64) {
        if let Some(limit) = &self.inner.state.limit {
            let current = self.inner.info.size.load(Relaxed);
            if size < current {
                limit.return_space(current - size);
            } else if size > current {
                limit.take_space(size - current);
            }
        }
        self.inner.info.size.store(size, Relaxed);
        self.inner.info.exists.store(true, Relaxed);
    }

    /// Set whether the related cache entry was used.
    fn set_used(&self) {
        self.inner.info.used.store(true, Relaxed);
    }
}

struct LiveEntryInner {
    state: Arc<State>,
    info: ManuallyDrop<Entry>,
}

impl Drop for LiveEntryInner {
    fn drop(&mut self) {
        self.state
            .add_inactive(unsafe { ManuallyDrop::take(&mut self.info) });
    }
}

struct State {
    inactive: Mutex<BinaryHeap<Entry>>,
    inactive_change: Notify,
    limit: Option<CacheLimit>,
    persist: std::path::PathBuf,
}

impl State {
    async fn evict<F: FnOnce(Key)>(&self, remove: F) {
        loop {
            let to_evict = self.inactive.lock().ok().and_then(|mut bh| bh.pop());
            if let Some(entry) = to_evict {
                log::debug!("evicting {}", entry.key);
                self.remove_entry(entry, remove);
                return;
            }
            self.inactive_change.notified().await;
        }
    }

    fn remove_entry<F: FnOnce(Key)>(&self, entry: Entry, remove: F) {
        let size = entry.size.load(Relaxed);
        remove(entry.key);
        if let Some(limit) = &self.limit {
            limit.return_space(size);
        }
    }

    fn active(self: &Arc<Self>, file: Key, channel: Channel) -> LiveEntry {
        LiveEntry {
            inner: Arc::new(LiveEntryInner {
                state: self.clone(),
                info: ManuallyDrop::new(Entry::new(file, channel)),
            }),
        }
    }

    fn add_inactive(&self, info: Entry) {
        if !info.exists.load(Relaxed) {
            // Return space without queueing the entry for eviction (there's no reason to keep it
            // around).
            self.remove_entry(info, |_| ());
            return;
        }
        if let Ok(mut guard) = self.inactive.lock() {
            guard.push(info);
        }
        self.inactive_change.notify_waiters();
    }
}

impl Drop for State {
    fn drop(&mut self) {
        let inactive: PersistedState = self
            .inactive
            .get_mut()
            .map(std::mem::take)
            .unwrap_or_default()
            .into_iter()
            .collect();
        if !inactive.is_empty() {
            if let Some(p) = self.persist.parent() {
                let _ = std::fs::create_dir_all(p);
            }
            match std::fs::File::create(&self.persist) {
                Ok(f) => {
                    if let Err(e) = serde_json::to_writer(f, &inactive) {
                        log::error!("failed to persist cache state: {e}");
                    }
                }
                Err(e) => log::error!("failed to create persistent cache state file: {e}"),
            }
        }
    }
}

type PersistedState = Vec<Entry>;

#[derive(Clone, Debug, Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Key {
    pub debug_name: String,
    pub debug_id: DebugId,
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.debug_id.breakpad(), self.debug_name)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Entry {
    key: Key,
    channel: Channel,
    /// The size of the debug file data on disk. 0 if unknown.
    size: AtomicU64,
    /// Whether this entry was referenced by the current set of data.
    #[serde(skip)]
    used: AtomicBool,
    /// Whether this entry was populated (i.e., whether the space allocated was actually used).
    #[serde(skip)]
    exists: AtomicBool,
}

impl Entry {
    fn new(key: Key, channel: Channel) -> Self {
        Entry {
            key,
            channel,
            size: 0.into(),
            used: false.into(),
            exists: false.into(),
        }
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.used.load(Relaxed) == other.used.load(Relaxed)
            && self.channel == other.channel
            && self.size.load(Relaxed) == other.size.load(Relaxed)
    }
}

impl Eq for Entry {}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Unused are ordered first
        self.used
            .load(Relaxed)
            .cmp(&other.used.load(Relaxed))
            // Lesser channels (e.g. nightly) are ordered first
            .then_with(|| self.channel.cmp(&other.channel))
            // Larger sizes are ordered first (greedy eviction)
            .then_with(|| {
                self.size
                    .load(Relaxed)
                    .cmp(&other.size.load(Relaxed))
                    .reverse()
            })
            // Things ordered less are ordered greater instead (to simplify use in `BinaryHeap`)
            .reverse()
    }
}

#[derive(
    Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq, serde::Serialize, serde::Deserialize,
)]
pub enum Channel {
    Nightly,
    Beta,
    Unknown,
    Release,
}

impl From<&str> for Channel {
    fn from(value: &str) -> Self {
        match value {
            "nightly" => Channel::Nightly,
            "beta" => Channel::Beta,
            "release" => Channel::Release,
            _ => Channel::Unknown,
        }
    }
}
