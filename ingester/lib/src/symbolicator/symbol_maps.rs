//! Manage the set of symbol maps needed for at task. The API ensures that all modules keys are
//! resolved concurrently, and that there are no lingering references (by e.g. holding the value
//! too long), which would prevent cache eviction.

use super::{cache, FileHelper, Location};
use anyhow::Context;
use futures_util::{
    future::{FutureExt, Shared},
    stream::FuturesUnordered,
};
use samply_symbols::{SymbolManager, SymbolMap};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::JoinHandle;

#[derive(Default)]
pub struct SymbolMaps {
    inner: Arc<std::sync::Mutex<HashMap<cache::Key, Shared<LoadSymbolMap>>>>,
}

pub struct SymbolMapGetter {
    inner: Arc<std::sync::Mutex<HashMap<cache::Key, Shared<LoadSymbolMap>>>>,
    manager: Arc<SymbolManager<FileHelper>>,
    job: cache::CacheJob,
    channel: cache::Channel,
}

pub(super) type LoadedSymbolMap = Arc<SymbolMap<FileHelper>>;

pub(super) type SymbolMapError = Arc<dyn std::error::Error + Send + Sync + 'static>;

struct LoadSymbolMap {
    task_handle: JoinHandle<anyhow::Result<LoadedSymbolMap>>,
}

impl std::future::Future for LoadSymbolMap {
    type Output = Result<LoadedSymbolMap, SymbolMapError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // # Safety
        // This is a wrapper over the JoinHandle and will not move.
        match unsafe { self.map_unchecked_mut(|this| &mut this.task_handle) }.poll(cx) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Err(e.into())),
            std::task::Poll::Ready(Ok(r)) => std::task::Poll::Ready(r),
        }
        .map_err(|e| Arc::from(Box::from(e)))
    }
}

impl SymbolMaps {
    pub fn getter(
        &self,
        manager: Arc<SymbolManager<FileHelper>>,
        cache: &cache::Cache,
        channel: cache::Channel,
    ) -> SymbolMapGetter {
        SymbolMapGetter {
            inner: self.inner.clone(),
            manager,
            job: cache.job(),
            channel,
        }
    }
}

impl SymbolMapGetter {
    pub fn get_symbol_maps<Extra>(
        self,
        keys: Vec<(Extra, cache::Key)>,
    ) -> FuturesUnordered<
        impl std::future::Future<Output = (Extra, Result<LoadedSymbolMap, SymbolMapError>)>,
    > {
        keys.into_iter()
            .map(|(extra, key)| self.get(key).map(move |map| (extra, map)))
            .collect()
    }

    fn get(&self, key: cache::Key) -> Shared<LoadSymbolMap> {
        self.inner
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_insert_with(|| {
                let manager = self.manager.clone();
                let location = Location {
                    key: key.clone(),
                    symindex: false,
                    cache_entry: self.job.register(key.clone(), self.channel),
                };
                LoadSymbolMap {
                    task_handle: tokio::spawn(async move {
                        // There's no need to explicitly hold the LiveEntry with the returned
                        // SymbolMap, because it stores the Location (including the LiveEntry)
                        // itself.
                        manager
                            .load_symbol_map_from_location(location.clone(), None)
                            .await
                            .map(Arc::new)
                            .with_context(|| format!("failed to load symbol map for {location}"))
                    }),
                }
                .shared()
            })
            .clone()
    }
}
