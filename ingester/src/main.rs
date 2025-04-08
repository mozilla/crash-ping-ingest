use anyhow::Context;
use clap::Parser;
use crash_ping_ingest::{Config, CrashPingIngest};
use progress::Progress;
use std::{
    io::{BufRead, BufReader, Write},
    path::PathBuf,
};

mod progress;

const DEFAULT_CONFIG_FILE: &str = "config.toml";

trait UnwrapOrLog {
    type Value;
    fn unwrap_or_log(self, value: Self::Value) -> Self::Value;
}

impl<T, E: std::error::Error> UnwrapOrLog for Result<T, E> {
    type Value = T;

    fn unwrap_or_log(self, value: Self::Value) -> Self::Value {
        match self {
            Ok(v) => v,
            Err(e) => {
                log::warn!("{}", e);
                value
            }
        }
    }
}

fn existing_path(s: &str) -> std::io::Result<PathBuf> {
    let p = PathBuf::from(s);
    if !std::fs::exists(&p)? {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file does not exist",
        ))
    } else {
        Ok(p)
    }
}

/// Crash symbolication.
///
/// All configuration is passed as TOML. In order from lowest to highest precedence: `config_file`
/// and command-line arguments can have TOML data which are merged to determine the final
/// configuration.
#[derive(Debug, Parser)]
struct Args {
    /// The path to the config file. If unspecified, uses `config.toml` (if it exists).
    #[arg(short, long, value_parser = existing_path)]
    config_file: Option<PathBuf>,

    /// The path from which to read jsonl input. If unspecified, input is read from stdin.
    #[arg(short, long)]
    input_file: Option<PathBuf>,

    /// The path to which to write the jsonl output. If unspecified, output is written to stdout.
    #[arg(short, long)]
    output_file: Option<PathBuf>,

    /// Disable progress display on stderr.
    #[arg(short = 'q', long)]
    no_progress: bool,

    /// Additional configuration to apply over the loaded config file (if any). Each argument is a
    /// line of TOML.
    config: Vec<String>,
}

fn toml_merge(target: &mut toml::Value, from: toml::Value) {
    use toml::Value::*;
    if from.same_type(target) {
        match from {
            Array(a) => {
                target.as_array_mut().unwrap().extend(a);
                return;
            }
            Table(t) => {
                let target = target.as_table_mut().unwrap();
                for (k, v) in t {
                    match target.entry(k) {
                        toml::map::Entry::Vacant(e) => {
                            e.insert(v);
                        }
                        toml::map::Entry::Occupied(e) => toml_merge(e.into_mut(), v),
                    }
                }
                return;
            }
            _ => (),
        }
    }
    *target = from;
}

struct JsonlReader<R, T> {
    reader: R,
    line: String,
    _phantom: std::marker::PhantomData<fn() -> T>,
}

impl<R, T> JsonlReader<R, T> {
    pub fn new(reader: R) -> Self {
        JsonlReader {
            reader,
            line: Default::default(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<R: BufRead, T: serde::de::DeserializeOwned> Iterator for JsonlReader<R, T> {
    type Item = anyhow::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.line.clear();
        match self.reader.read_line(&mut self.line) {
            Ok(0) => None,
            Err(e) => Some(Err(e.into())),
            Ok(_) => Some(serde_json::from_str(&self.line).map_err(|e| e.into())),
        }
    }
}

fn main() -> anyhow::Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Warn)
        .parse_default_env()
        .init();

    let mut args = Args::parse();

    if args.config_file.is_none() && std::fs::exists(DEFAULT_CONFIG_FILE).unwrap_or_log(false) {
        args.config_file = Some(DEFAULT_CONFIG_FILE.into());
    }

    let toml_config: toml::Table = if let Some(file) = args.config_file {
        std::fs::read_to_string(&file)
            .map_err(|e| anyhow::Error::from(e))
            .and_then(|s| Ok(toml::from_str(&s)?))
            .with_context(|| format!("while reading {}", file.display()))?
    } else {
        Default::default()
    };
    let mut toml_config: toml::Value = toml_config.into();

    let cli_config: toml::Table =
        toml::from_str(&args.config.join("\n")).context("while parsing CLI TOML arguments")?;

    // Merge configs
    toml_merge(&mut toml_config, cli_config.into());

    let config: Config = toml_config.try_into()?;

    let input = args
        .input_file
        .map(|path| {
            std::fs::File::open(path).map(|f| Box::new(BufReader::new(f)) as Box<dyn BufRead>)
        })
        .unwrap_or_else(|| Ok(Box::new(std::io::stdin().lock())))?;

    let mut output = args
        .output_file
        .map(|path| std::fs::File::create(path).map(|f| Box::new(f) as Box<dyn Write>))
        .unwrap_or_else(|| Ok(Box::new(std::io::stdout())))?;

    let ingest = CrashPingIngest::new(config, JsonlReader::new(input), move |ping_info| {
        serde_json::to_writer(&mut output, &ping_info)?;
        writeln!(&mut output)?;
        Ok(())
    });

    let cancellation_status = ingest.status.clone();
    ctrlc::set_handler(move || cancellation_status.cancel())
        .expect("failed to set interrupt handler");
    let _progress = if args.no_progress {
        None
    } else {
        Progress::new(ingest.status.clone())
    };

    ingest.run()
}
