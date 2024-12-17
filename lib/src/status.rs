//! Status tracking.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
    Mutex,
};

pub struct Status {
    pub queries: Queries,
    pub pings: Pings,
    pub cache: Option<Cache>,
    pub(crate) cancel: Cancel,
}

#[derive(Default)]
pub struct Queries {
    complete: AtomicUsize,
    total: AtomicUsize,
}

#[derive(Default)]
pub struct Pings {
    symbolicating: AtomicUsize,
    complete: AtomicUsize,
    total: AtomicUsize,
}

#[derive(Default)]
pub struct Cache {
    get_current: Mutex<Option<Box<dyn Fn() -> u64 + Send>>>,
    total: u64,
}

#[derive(Default)]
pub(crate) struct Cancel {
    cancelled: AtomicBool,
    on_cancel: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl Status {
    pub(crate) fn new(config: &crate::Config) -> Self {
        Status {
            queries: Default::default(),
            pings: Default::default(),
            cache: config.cache.size_limit_gb.limit_bytes().map(|total| Cache {
                get_current: Default::default(),
                total,
            }),
            cancel: Default::default(),
        }
    }

    /// Cancel execution.
    pub fn cancel(&self) {
        self.cancel.cancelled.store(true, Relaxed);
        if let Ok(mut guard) = self.cancel.on_cancel.lock() {
            if let Some(f) = guard.take() {
                f();
            }
        }
    }

    /// Return whether execution has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
}

impl Queries {
    pub(crate) fn inc_complete(&self) {
        self.complete.fetch_add(1, Relaxed);
    }

    pub fn complete_count(&self) -> usize {
        self.complete.load(Relaxed)
    }

    pub(crate) fn set_total(&self, val: usize) {
        self.total.store(val, Relaxed)
    }

    pub fn total_count(&self) -> usize {
        self.total.load(Relaxed)
    }

    pub fn done(&self) -> bool {
        self.complete_count() == self.total_count()
    }
}

impl Pings {
    pub(crate) fn inc_symbolicating(&self) {
        self.symbolicating.fetch_add(1, Relaxed);
    }

    pub(crate) fn dec_symbolicating(&self) {
        self.symbolicating.fetch_sub(1, Relaxed);
    }

    pub fn symbolicating_count(&self) -> usize {
        self.symbolicating.load(Relaxed)
    }

    pub(crate) fn inc_complete(&self) {
        self.complete.fetch_add(1, Relaxed);
    }

    pub fn complete_count(&self) -> usize {
        self.complete.load(Relaxed)
    }

    pub(crate) fn inc_total(&self, val: usize) {
        self.total.fetch_add(val, Relaxed);
    }

    pub fn total_count(&self) -> usize {
        self.total.load(Relaxed)
    }

    pub fn done(&self) -> bool {
        self.complete_count() == self.total_count()
    }
}

impl Cache {
    pub(crate) fn set_current_callback<F: Fn() -> u64 + Send + 'static>(&self, f: F) {
        if let Ok(mut guard) = self.get_current.lock() {
            *guard = Some(Box::new(f));
        }
    }

    pub fn current(&self) -> u64 {
        if let Ok(guard) = self.get_current.lock() {
            if let Some(f) = &*guard {
                return f();
            }
        }
        0
    }

    pub fn max_size(&self) -> u64 {
        self.total
    }
}

impl Cancel {
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Relaxed)
    }

    pub fn on_cancel<F: FnOnce() + Send + 'static>(&self, f: F) {
        if let Ok(mut guard) = self.on_cancel.lock() {
            if self.is_cancelled() {
                drop(guard);
                f();
            } else {
                *guard = Some(Box::new(f));
            }
        }
    }
}
