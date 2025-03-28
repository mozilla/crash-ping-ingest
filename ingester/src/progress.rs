use crash_ping_ingest::Status;
use std::mem::ManuallyDrop;
use std::sync::{
    atomic::{AtomicBool, Ordering::Relaxed},
    Arc,
};
use std::thread;

const RENDER_FREQUENCY: std::time::Duration = std::time::Duration::from_millis(100);

pub struct Progress {
    cancel: Arc<AtomicBool>,
    thread: ManuallyDrop<thread::JoinHandle<()>>,
}

impl Progress {
    pub fn new(status: Arc<Status>) -> Option<Self> {
        let cancel = Arc::new(AtomicBool::new(false));
        let mut renderer = Renderer::new(status)?;
        Some(Progress {
            cancel: cancel.clone(),
            thread: ManuallyDrop::new(thread::spawn(move || {
                while !cancel.load(Relaxed) {
                    if let Err(e) = renderer.render() {
                        log::warn!("failed to render to terminal: {e}");
                    }
                    thread::sleep(RENDER_FREQUENCY);
                }
            })),
        })
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        self.cancel.store(true, Relaxed);
        unsafe { ManuallyDrop::take(&mut self.thread) }
            .join()
            .unwrap();
    }
}

struct Renderer {
    terminal: Box<term::StderrTerminal>,
    status: Arc<Status>,
    last_lines: usize,
}

impl Renderer {
    fn new(status: Arc<Status>) -> Option<Self> {
        Some(Renderer {
            terminal: term::stderr()?,
            status,
            last_lines: 0,
        })
    }

    fn render(&mut self) -> term::Result<()> {
        // Reset from last render
        for i in 0..std::mem::replace(&mut self.last_lines, 0) {
            if i == 0 {
                self.terminal.carriage_return()?;
            } else {
                self.terminal.cursor_up()?;
            }
            self.terminal.delete_line()?;
        }

        if !self.status.pings.done() {
            if self.last_lines > 0 {
                writeln!(self.terminal)?;
            }
            let complete = self.status.pings.complete_count();
            let total = self.status.pings.total_count();
            write!(
                self.terminal,
                "Pings: {:.1}% ({}/{}), {} symbolicating",
                complete as f64 * 100. / total as f64,
                complete,
                total,
                self.status.pings.symbolicating_count()
            )?;
            self.last_lines += 1;
        }

        if let Some(cache) = self.status.cache.as_ref() {
            if self.last_lines > 0 {
                writeln!(self.terminal)?;
            }
            let current = cache.current();
            let max = cache.max_size();
            write!(
                self.terminal,
                "Cache usage: {:.1}% of {}",
                current as f64 * 100. / max as f64,
                friendly_byte_units(cache.max_size()),
            )?;
            self.last_lines += 1;
        }

        if self.status.is_cancelled() {
            if self.last_lines > 0 {
                writeln!(self.terminal)?;
            }
            write!(self.terminal, "Cancelling...")?;
            self.last_lines += 1;
        }

        Ok(())
    }
}

fn friendly_byte_units(mut size: u64) -> String {
    const UNITS: &[&str] = &["B", "kB", "MB", "GB", "TB"];
    const FACTOR: u64 = 1000;

    let mut ind = 0;
    while size > FACTOR {
        size /= FACTOR;
        ind += 1;
    }

    format!("{}{}", size, UNITS[ind])
}
