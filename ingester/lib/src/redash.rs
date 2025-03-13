use super::Config;
use anyhow::Context;
use futures_util::{future::BoxFuture, FutureExt};
use reqwest::{header, Client};
use std::collections::BTreeMap;

const REDASH_URL: &str = "https://sql.telemetry.mozilla.org";

#[derive(Debug, serde::Serialize)]
struct Payload {
    max_age: Option<usize>,
    parameters: BTreeMap<String, String>,
}

#[derive(PartialEq, Debug, serde_repr::Deserialize_repr)]
#[repr(u8)]
enum JobStatus {
    Pending = 1,
    Started = 2,
    Success = 3,
    Failure = 4,
    Cancelled = 5,
}

#[derive(Debug, serde::Deserialize)]
enum ApiResult {
    #[serde(rename = "job")]
    Job {
        status: JobStatus,
        id: Id,
        query_result_id: Option<Id>,
        error: Option<String>,
    },
    #[serde(rename = "query_result")]
    QueryResult { data: QueryResultData },
}

// For some reason the API returns both numeric and UUID ids.
#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
enum Id {
    Int(u64),
    String(String),
}

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int(i) => i.fmt(f),
            Self::String(s) => s.fmt(f),
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct QueryResultData {
    rows: Vec<QueryRow>,
}

#[derive(Debug, serde::Deserialize)]
pub struct QueryRow {
    pub document_id: String,
    pub submission_timestamp: String,
    pub stack_traces: Option<String>,
    pub java_exception: Option<String>,
    pub moz_crash_reason: Option<String>,
    pub ipc_channel_error: Option<String>,
    pub oom_size: Option<u64>,
    pub normalized_os: Option<String>,
}

#[derive(Debug)]
pub struct Query {
    pub parameters: BTreeMap<String, String>,
    pub response_rows: Vec<QueryRow>,
}

/// Create redash requests based on the configuration.
pub fn create_requests(
    config: &Config,
) -> anyhow::Result<Vec<BoxFuture<'_, anyhow::Result<Query>>>> {
    let client = Client::builder()
        .user_agent(super::APP_USER_AGENT)
        .default_headers(
            [(
                header::AUTHORIZATION,
                format!("Key {}", config.redash.api_key).parse()?,
            )]
            .into_iter()
            .collect(),
        )
        .build()?;

    let query_id = config.redash.query_id;
    Ok(config
        .redash
        .query_parameter_sets()
        .map(move |parameters| {
            let client = client.clone();
            async move {
                let mut result = client
                    .post(format!("{REDASH_URL}/api/queries/{query_id}/results"))
                    .json(&Payload {
                        max_age: config.redash.max_age_seconds,
                        parameters: parameters.clone(),
                    })
                    .send()
                    .await;
                loop {
                    let response = result
                        .and_then(|r| r.error_for_status())
                        .context("error querying redash")?;
                    match response.json().await? {
                        ApiResult::QueryResult { data } => {
                            return Ok(Query {
                                parameters,
                                response_rows: data.rows,
                            });
                        }
                        ApiResult::Job {
                            status: JobStatus::Pending | JobStatus::Started,
                            id,
                            ..
                        } => {
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            result = client
                                .get(format!("{REDASH_URL}/api/jobs/{id}"))
                                .send()
                                .await;
                        }
                        ApiResult::Job {
                            status: JobStatus::Success,
                            query_result_id,
                            id,
                            ..
                        } => {
                            let result_id = query_result_id.with_context(|| {
                                format!("successful job ({id}) missing query_result_id")
                            })?;
                            result = client
                                .get(format!(
                                    "{REDASH_URL}/api/queries/{query_id}/results/{result_id}.json"
                                ))
                                .send()
                                .await;
                        }
                        ApiResult::Job {
                            status: JobStatus::Failure,
                            id,
                            error,
                            ..
                        } => {
                            anyhow::bail!(
                                "job {id} failed: {}",
                                error.as_deref().unwrap_or("unknown error")
                            );
                        }
                        ApiResult::Job {
                            status: JobStatus::Cancelled,
                            id,
                            ..
                        } => {
                            anyhow::bail!("job {id} cancelled");
                        }
                    }
                }
            }
            .boxed()
        })
        .collect())
}
