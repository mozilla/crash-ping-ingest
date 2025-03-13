pub use config::Config;
use futures_util::{stream::FuturesUnordered, StreamExt};
pub use status::Status;
use std::sync::Arc;
use symbolicator::Symbolicator;
use tokio::runtime;
use tokio::task::JoinHandle;

pub mod config;
mod redash;
mod signature;
pub mod status;
mod symbolicator;

const APP_USER_AGENT: &str = "crash-ping-ingest/1.0";
const ONLY_SYMBOLICATE_CRASHING_THREAD: bool = true;

pub struct CrashPingIngest {
    pub status: Arc<Status>,
    config: Config,
}

impl CrashPingIngest {
    pub fn new(config: Config) -> Self {
        let status = Arc::new(Status::new(&config));
        CrashPingIngest { status, config }
    }

    pub fn run<F: FnMut(PingInfo) -> anyhow::Result<()>>(
        self,
        mut output: F,
    ) -> anyhow::Result<()> {
        let CrashPingIngest { status, config } = self;

        log::info!("configuration: {config:#?}");
        log::info!(
            "{} redash queries generated",
            config.redash.query_parameter_sets().count()
        );

        log::debug!(
            "expanded queries: {:#?}",
            config.redash.query_parameter_sets().collect::<Vec<_>>()
        );

        let mut builder = runtime::Builder::new_multi_thread();
        builder.enable_all().thread_name("crash-symbolicate");

        if let config::WorkerThreads::Exact(n) = config.worker_threads {
            builder.worker_threads(n);
        }

        builder.build()?.block_on(async move {
            let mut requests = FuturesUnordered::from_iter(redash::create_requests(&config)?);
            status.queries.set_total(requests.len());
            let symbolicator =
                Symbolicator::new(&config, &status, ONLY_SYMBOLICATE_CRASHING_THREAD)?;
            let signature_generator = signature::Generator::new(&config.signature);

            let mut results: Vec<JoinHandle<PingInfo>> = Default::default();

            while let Some(result) = requests.next().await {
                status.queries.inc_complete();
                match result {
                    Err(e) => log::warn!("request failed: {e:#}"),
                    Ok(query) => {
                        status.pings.inc_total(query.response_rows.len());

                        let channel = query
                            .parameters
                            .get("channel")
                            .cloned()
                            .unwrap_or("unknown".into());

                        for mut row in query.response_rows {
                            let symbolicated_frames =
                                row.stack_traces.take().and_then(|stack_traces| {
                                    // Rather than adding an `Option` to the already complex
                                    // `symbolicate` method, just check for a "null" value here.
                                    (stack_traces != "null").then(|| {
                                        symbolicator.symbolicate(stack_traces, channel.clone())
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

                                let signature =
                                    match signature_generator.generate(&symbolicated, &row).await {
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
                    }
                }
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
