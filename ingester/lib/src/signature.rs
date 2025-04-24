//! Signature generation functionality.
//!
//! This currently requires an additional python program to perform signature generation. We could
//! embed a python interpreter here, but because of the GIL it won't be much more convenient than
//! calling externally.

use crate::config::Signature;
use crate::symbolicator::Symbolicated;
use crate::InputRow;
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
        ping_info: &InputRow,
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
            os: ping_info.os.as_deref(),
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

        let mut module: Option<String> = None;
        let mut exc_type: Option<String> = None;

        let mut parse_exception = |s: &str| {
            let mut iter = s.rsplitn(2, ".");
            exc_type = iter.next().map(ToOwned::to_owned);
            module = iter.next().map(ToOwned::to_owned);
        };

        let frames;
        match exception {
            json::JavaException::V0 { messages, stack } => {
                // Try to get exception name from the messages.
                let first_message = messages.into_iter().nth(0);
                if let Some((front, _)) = first_message.as_ref().and_then(|m| m.split_once(":")) {
                    if front.ends_with("Exception") {
                        parse_exception(front);
                    }
                }
                frames = stack;
            }
            json::JavaException::V1 { throwables } => {
                if let Some(first) = throwables.into_iter().nth(0) {
                    parse_exception(&first.type_name);
                    frames = first.stack;
                } else {
                    frames = Default::default();
                }
            }
        }

        Some(JavaException {
            exception: Exception {
                values: vec![ExceptionValue::Stacktrace {
                    module,
                    exc_type,
                    frames,
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
    #[serde(untagged)]
    pub enum JavaException<'a> {
        V0 {
            #[serde(default, borrow)]
            stack: Vec<super::ExceptionFrame<'a>>,
            #[serde(default, borrow)]
            messages: Vec<Cow<'a, str>>,
        },
        V1 {
            #[serde(default, borrow)]
            throwables: Vec<Throwable<'a>>,
        },
    }

    #[derive(Debug, Deserialize)]
    pub struct Throwable<'a> {
        #[serde(rename(deserialize = "typeName"))]
        pub type_name: Cow<'a, str>,
        #[serde(default, borrow)]
        pub stack: Vec<super::ExceptionFrame<'a>>,
    }
}
