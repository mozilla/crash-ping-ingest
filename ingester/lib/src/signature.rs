//! Signature generation functionality.
//!
//! This currently requires an additional python program to perform signature generation. We could
//! embed a python interpreter here, but because of the GIL it won't be much more convenient than
//! calling externally.

use crate::config::Signature;
use crate::redash::QueryRow;
use crate::symbolicator::Symbolicated;
use std::borrow::Cow;
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct Generator {
    inner: Arc<GeneratorInner>,
}

struct GeneratorInner {
    binary: String,
    permits: Semaphore,
}

impl Generator {
    pub fn new(config: &Signature) -> Self {
        Generator {
            inner: Arc::new(GeneratorInner {
                binary: config.generator.clone(),
                permits: Semaphore::new(tokio::runtime::Handle::current().metrics().num_workers()),
            }),
        }
    }

    pub async fn generate(
        &self,
        symbolicated: &Option<Symbolicated>,
        ping_info: &QueryRow,
    ) -> anyhow::Result<String> {
        let java_exception = ping_info
            .java_exception
            .as_deref()
            .and_then(JavaException::from_json);

        let input = Input {
            symbolicated,
            java_exception,
            oom_allocation_size: ping_info.oom_size,
            ipc_channel_error: ping_info.ipc_channel_error.as_deref(),
            moz_crash_reason: ping_info.moz_crash_reason.as_deref(),
            os: ping_info.normalized_os.as_deref(),
        };

        let input = serde_json::to_string(&input)?;

        let output = {
            let _permit = self.inner.permits.acquire().await;
            // Intentionally use a non-async-aware Command, because (aside from stdio) we expect
            // the invoked process to be CPU-bound (so we want it to "consume" the current thread
            // to appropriately match the allocated resources).
            let mut command = tokio::process::Command::new(&self.inner.binary);
            command
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            // Set the child process group to itself so that it won't receive SIGINT and fail (we
            // expect this process to run quite quickly, so we'll always wait for it to complete).
            #[cfg(unix)]
            command.process_group(0);
            let mut child = command.spawn()?;

            {
                use tokio::io::AsyncWriteExt;
                let mut stdin = child.stdin.take().unwrap();
                stdin.write_all(input.as_bytes()).await?;
            }

            child.wait_with_output().await?
        };

        anyhow::ensure!(
            output.status.success(),
            "signature generation program returned non-zero exit code ({}); stderr: {}",
            output
                .status
                .code()
                .map(|i| Cow::Owned(i.to_string()))
                .unwrap_or(Cow::Borrowed("none")),
            String::from_utf8_lossy(&output.stderr)
        );

        serde_json::from_slice(&output.stdout)
            .map(|o: Output| o.signature)
            .map_err(|e| e.into())
    }
}

#[derive(Debug, serde::Deserialize)]
struct Output {
    signature: String,
}

#[derive(Debug, serde::Serialize)]
struct Input<'a> {
    #[serde(flatten)]
    symbolicated: &'a Option<Symbolicated>,
    java_exception: Option<JavaException<'a>>,
    oom_allocation_size: Option<u64>,
    ipc_channel_error: Option<&'a str>,
    moz_crash_reason: Option<&'a str>,
    os: Option<&'a str>,
}

#[derive(Debug, serde::Serialize)]
struct JavaException<'a> {
    exception: Exception<'a>,
}

impl<'a> JavaException<'a> {
    fn from_json(json: &'a str) -> Option<Self> {
        let exception = match serde_json::from_str::<Option<json::JavaException>>(json) {
            Err(e) => {
                log::error!("failed to deserialize java exception: {e}");
                None
            }
            Ok(v) => v,
        }?;

        // Try to get exception name from the messages.
        let mut module = None;
        let mut exc_type = None;
        let first_message = exception.messages.into_iter().nth(0);
        if let Some((front, _)) = first_message.as_ref().and_then(|m| m.split_once(":")) {
            if front.ends_with("Exception") {
                let mut iter = front.rsplitn(2, ".");
                exc_type = iter.next().map(ToOwned::to_owned);
                module = iter.next().map(ToOwned::to_owned);
            }
        }

        Some(JavaException {
            exception: Exception {
                values: vec![ExceptionValue::Stacktrace {
                    module,
                    exc_type,
                    frames: exception.stack,
                }],
            },
        })
    }
}

#[derive(Debug, serde::Serialize)]
struct Exception<'a> {
    values: Vec<ExceptionValue<'a>>,
}

#[derive(Debug, serde::Serialize)]
enum ExceptionValue<'a> {
    #[serde(rename = "stacktrace")]
    Stacktrace {
        module: Option<String>,
        #[serde(rename = "type")]
        exc_type: Option<String>,
        frames: Vec<ExceptionFrame<'a>>,
    },
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ExceptionFrame<'a> {
    #[serde(rename(deserialize = "className"))]
    module: Option<Cow<'a, str>>,
    #[serde(rename(deserialize = "methodName"))]
    function: Option<Cow<'a, str>>,
    #[serde(rename(deserialize = "file"))]
    filename: Option<Cow<'a, str>>,
}

mod json {
    use serde::Deserialize;
    use std::borrow::Cow;

    #[derive(Debug, Deserialize)]
    pub struct JavaException<'a> {
        #[serde(default, borrow)]
        pub stack: Vec<super::ExceptionFrame<'a>>,
        #[serde(default, borrow)]
        pub messages: Vec<Cow<'a, str>>,
    }
}
