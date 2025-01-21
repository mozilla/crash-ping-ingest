pub use config::Config;
use futures_util::{
    future::{try_join_all, FutureExt},
    stream::FuturesUnordered,
    StreamExt,
};
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

pub struct CrashPingIngest {
    pub status: Arc<Status>,
    config: Config,
}

impl CrashPingIngest {
    pub fn new(config: Config) -> Self {
        let status = Arc::new(Status::new(&config));
        CrashPingIngest { status, config }
    }

    pub fn run(self) -> anyhow::Result<Vec<PingInfo>> {
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
            let symbolicator = Symbolicator::new(&config, &status)?;
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
                            let mut parameters = query.parameters.clone();
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
                                            s.crashing_thread.and_then(|ind| {
                                                s.threads.into_iter().nth(ind).map(|t| {
                                                    t.frames
                                                        .into_iter()
                                                        .enumerate()
                                                        .map(|(index, f)| StackFrame {
                                                            index: index as u64,
                                                            module: f.module,
                                                            frame: f.function,
                                                            src_url: None,
                                                        })
                                                        .collect()
                                                })
                                            }),
                                        )
                                    })
                                    .unwrap_or_default();

                                status.pings.inc_complete();

                                PingInfo {
                                    channel: parameters.remove("channel").unwrap(),
                                    process: parameters.remove("process_type").unwrap(),
                                    ipc_actor: parameters
                                        .remove("utility_actor")
                                        .and_then(|s| (s != "NONE").then_some(s)),
                                    clientid: row.client_id,
                                    crashid: row.document_id,
                                    version: row.display_version,
                                    os: row.normalized_os,
                                    osversion: row.normalized_os_version,
                                    arch: row.arch,
                                    date: row.crash_time,
                                    reason: row.moz_crash_reason,
                                    crash_type,
                                    minidump_sha256_hash: row.minidump_sha256_hash,
                                    startup_crash: row.startup_crash,
                                    build_id: row.build_id,
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

            // Ignore cancelled tasks
            let results = try_join_all(results.into_iter().map(|join_handle| {
                join_handle.map(|result| match result {
                    Err(e) if e.is_cancelled() => Ok(None),
                    Err(e) => Err(e),
                    Ok(v) => Ok(Some(v)),
                })
            }));

            tokio::select! {
                _ = symbolicator.finish_tasks() => unreachable!(),
                ping_infos = results => Ok(ping_infos?.into_iter().flatten().collect::<Vec<_>>()),
            }
        })
    }
}

#[derive(Debug, serde::Serialize)]
pub struct PingInfo {
    pub channel: String,
    pub process: String,
    pub ipc_actor: Option<String>,
    pub clientid: String,
    pub crashid: String,
    pub version: Option<String>,
    pub os: Option<String>,
    pub osversion: Option<String>,
    pub arch: Option<String>,
    pub date: Option<String>,
    pub reason: Option<String>,
    #[serde(rename = "type")]
    pub crash_type: Option<String>,
    pub minidump_sha256_hash: Option<String>,
    pub startup_crash: Option<bool>,
    pub build_id: Option<String>,

    // Derived data
    pub signature: Option<String>,
    pub stack: Option<Vec<StackFrame>>,
}

#[derive(Debug, serde::Serialize)]
pub struct StackFrame {
    index: u64,
    module: Option<String>,
    frame: Option<String>,
    #[serde(rename = "srcUrl")]
    src_url: Option<String>,
}
