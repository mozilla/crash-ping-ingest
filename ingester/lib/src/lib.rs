pub use config::Config;
use futures_util::{stream::FuturesUnordered, StreamExt};
pub use status::Status;
use std::sync::Arc;
use symbolicator::Symbolicator;
use tokio::runtime;
use tokio::task::JoinHandle;

pub mod config;
mod signature;
pub mod status;
mod symbolicator;

const APP_USER_AGENT: &str = "crash-ping-ingest/1.0";
const ONLY_SYMBOLICATE_CRASHING_THREAD: bool = true;

pub struct CrashPingIngest<Input, Output> {
    pub status: Arc<Status>,
    config: Config,
    input: Input,
    output: Output,
}

impl<I, O> CrashPingIngest<I, O>
where
    I: Iterator<Item = anyhow::Result<InputRow>>,
    O: FnMut(PingInfo) -> anyhow::Result<()>,
{
    pub fn new(config: Config, input: I, output: O) -> Self {
        let status = Arc::new(Status::new(&config));
        CrashPingIngest {
            status,
            config,
            input,
            output,
        }
    }
}

impl<I, O> CrashPingIngest<I, O>
where
    I: Iterator<Item = anyhow::Result<InputRow>>,
    O: FnMut(PingInfo) -> anyhow::Result<()>,
{
    pub fn run(self) -> anyhow::Result<()> {
        let CrashPingIngest {
            status,
            config,
            mut input,
            mut output,
        } = self;

        log::info!("configuration: {config:#?}");

        let mut builder = runtime::Builder::new_multi_thread();
        builder.enable_all().thread_name("crash-symbolicate");

        if let config::WorkerThreads::Exact(n) = config.worker_threads {
            builder.worker_threads(n);
        }

        builder.build()?.block_on(async move {
            let symbolicator =
                Symbolicator::new(&config, &status, ONLY_SYMBOLICATE_CRASHING_THREAD)?;
            let signature_generator = signature::Generator::new(&config.signature);

            let mut results: Vec<JoinHandle<PingInfo>> = Default::default();

            while let Some(mut row) = input.next().transpose()? {
                status.pings.inc_total();
                let symbolicated_frames = row.stack_traces.take().and_then(|stack_traces| {
                    // Rather than adding an `Option` to the already complex
                    // `symbolicate` method, just check for a "null" value here.
                    (stack_traces != "null").then(|| {
                        symbolicator
                            .symbolicate(stack_traces, row.channel.as_deref().unwrap_or("unknown"))
                    })
                });
                if symbolicated_frames.is_some() {
                    // Increment the status here to better represent the work to be
                    // done. If done in the task, it may not run for a long while.
                    status.pings.inc_symbolicating();
                }
                let signature_generator = signature_generator.clone();
                let status = status.clone();
                results.push(tokio::spawn(async move {
                    let symbolicated = if let Some(fut) = symbolicated_frames {
                        let result = fut.await;
                        status.pings.dec_symbolicating();
                        match result {
                            Err(e) => {
                                log::info!("failed to get symbolicated frames: {e}");
                                None
                            }
                            Ok(v) => Some(v),
                        }
                    } else {
                        None
                    };

                    let signature = match signature_generator.generate(&symbolicated, &row).await {
                        Ok(s) => Some(s),
                        Err(e) => {
                            log::error!("error generating signature: {e:#}");
                            None
                        }
                    };

                    let (crash_type, stack) = symbolicated
                        .map(|s| {
                            (
                                s.reason,
                                if ONLY_SYMBOLICATE_CRASHING_THREAD {
                                    s.threads.into_iter().next().map(|t| t.frames)
                                } else {
                                    s.crashing_thread.and_then(|ind| {
                                        s.threads.into_iter().nth(ind).map(|t| t.frames)
                                    })
                                },
                            )
                        })
                        .unwrap_or_default();

                    status.pings.inc_complete();

                    PingInfo {
                        document_id: row.document_id,
                        submission_timestamp: row.submission_timestamp,
                        crash_type,
                        signature,
                        stack,
                    }
                }));
            }

            {
                let aborts = results.iter().map(|j| j.abort_handle()).collect::<Vec<_>>();
                status.cancel.on_cancel(move || {
                    aborts.into_iter().for_each(|a| a.abort());
                });
            }

            log::info!(
                "processing {} pings (symbolicating {})",
                status.pings.total_count(),
                status.pings.symbolicating_count()
            );

            let mut results = FuturesUnordered::from_iter(results);
            let output_results = async move {
                while let Some(result) = results.next().await {
                    match result {
                        // Ignore cancelled tasks
                        Err(e) if e.is_cancelled() => (),
                        Err(e) => return Err(e.into()),
                        Ok(ping_info) => output(ping_info)?,
                    }
                }
                Ok(())
            };

            tokio::select! {
                _ = symbolicator.finish_tasks() => unreachable!(),
                result = output_results => result
            }
        })
    }
}

#[derive(Debug, serde::Serialize)]
pub struct PingInfo {
    pub document_id: String,
    pub submission_timestamp: String,
    pub crash_type: Option<String>,
    pub signature: Option<String>,
    pub stack: Option<Vec<symbolicator::FrameInfo>>,
}

#[derive(Debug, serde::Deserialize)]
pub struct InputRow {
    pub document_id: String,
    pub submission_timestamp: String,
    pub stack_traces: Option<String>,
    pub java_exception: Option<String>,
    pub moz_crash_reason: Option<String>,
    pub ipc_channel_error: Option<String>,
    pub oom_size: Option<u64>,
    pub os: Option<String>,
    pub channel: Option<String>,
}
