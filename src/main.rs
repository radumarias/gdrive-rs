use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::{io, panic, process};

use clap::{crate_authors, crate_name, crate_version, Arg, ArgAction, ArgMatches, Command};
use ctrlc::set_handler;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::task;
use tracing::level_filters::LevelFilter;
use tracing::{error, info, warn, Level};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

use anyhow::Result;
use gdrive_rs::mount::MountPoint;
use gdrive_rs::{is_debug, mount};

#[derive(Debug, Error)]
enum ExitStatusError {
    #[error("exit with status {0}")]
    Failure(i32),
}

#[tokio::main]
async fn main() -> Result<()> {
    let matches = get_cli_args();

    let str = matches.get_one::<String>("log-level").unwrap().as_str();
    let log_level = Level::from_str(str);
    if log_level.is_err() {
        panic!("Invalid log level");
    }
    let log_level = log_level.unwrap();
    let guard = log_init(log_level);

    let mount_point = match matches.subcommand() {
        Some(("mount", matches)) => {
            Some(matches.get_one::<String>("mount-point").unwrap().as_str())
        }
        _ => None,
    };

    let res = task::spawn_blocking(|| {
        panic::catch_unwind(|| {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(async { async_main().await })
        })
    })
    .await;
    match res {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(err))) => {
            let err2 = err.downcast_ref::<ExitStatusError>();
            if let Some(ExitStatusError::Failure(code)) = err2 {
                info!("Bye!");
                drop(guard);
                process::exit(*code);
            }
            error!("{err}");
            if let Some(mount_point) = mount_point {
                let _ = umount(mount_point).map_err(|err| {
                    warn!("Cannot umount, maybe it was not mounted: {err}");
                    err
                });
            }
            Err(err)
        }
        Ok(Err(err)) => {
            error!("{err:#?}");
            if let Some(mount_point) = mount_point {
                let _ = umount(mount_point).map_err(|err| {
                    warn!("Cannot umount, maybe it was not mounted: {err}");
                    err
                });
            }
            drop(guard);
            panic!("{err:#?}");
        }
        Err(err) => {
            error!("{err}");
            if let Some(mount_point) = mount_point {
                let _ = umount(mount_point).map_err(|err| {
                    warn!("Cannot umount, maybe it was not mounted: {err}");
                    err
                });
            }
            drop(guard);
            panic!("{err}");
        }
    }
}

#[allow(clippy::too_many_lines)]
fn get_cli_args() -> ArgMatches {
    Command::new(crate_name!())
        .version(crate_version!())
        .author(crate_authors!())
        .arg_required_else_help(true)
        .arg(
            Arg::new("log-level")
                .long("log-level")
                .short('l')
                .value_name("log-level")
                .default_value("INFO")
                .help("Log level, possible values: TRACE, DEBUG, INFO, WARN, ERROR"),
        )
        .arg(
            Arg::new("mount-point")
                .long("mount-point")
                .short('m')
                .required(true)
                .value_name("MOUNT_POINT")
                .help("Act as a client, and mount FUSE at given path"),
        )
        .arg(
            Arg::new("umount-on-start")
                .long("umount-on-start")
                .short('u')
                .action(ArgAction::SetTrue)
                .help("If we should try to umount the mountpoint before starting the FUSE server. This can be useful when the previous run crashed or was forced kll and the mountpoint is still mounted."),
        )
        .arg(
            Arg::new("allow-root")
                .long("allow-root")
                .short('r')
                .action(ArgAction::SetTrue)
                .help("Allow root user to access filesystem"),
        )
        .arg(
            Arg::new("allow-other")
                .long("allow-other")
                .short('o')
                .action(ArgAction::SetTrue)
                .help("Allow other user to access filesystem"),
        )
        .arg(
            Arg::new("direct-io")
                .long("direct-io")
                .short('i')
                .action(ArgAction::SetTrue)
                .requires("mount-point")
                .help("Use direct I/O (bypass page cache for an open file)"),
        )
        .arg(
            Arg::new("suid")
                .long("suid")
                .short('s')
                .action(ArgAction::SetTrue)
                .help("If it should allow setting SUID and SGID when files are created. Default is false and it will unset those flags when creating files"),
        )
        .get_matches()
}

#[allow(clippy::missing_panics_doc)]
pub fn log_init(level: Level) -> WorkerGuard {
    let directive = format!("gdrive_rust={}", level.as_str())
        .parse()
        .expect("cannot parse log directive");
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env()
        .unwrap()
        .add_directive(directive);

    let (writer, guard) = tracing_appender::non_blocking(io::stdout());
    let builder = tracing_subscriber::fmt()
        .with_writer(writer)
        .with_env_filter(filter);
    // .with_max_level(level);
    if is_debug() {
        builder.pretty().init();
    } else {
        builder.init();
    }

    guard
}

async fn async_main() -> anyhow::Result<()> {
    let matches = get_cli_args();
    run_mount(&matches).await?;
    Ok(())
}

async fn run_mount(matches: &ArgMatches) -> Result<()> {
    let mountpoint: String = matches
        .get_one::<String>("mount-point")
        .unwrap()
        .to_string();

    if matches.get_flag("umount-on-start") {
        let _ = umount(mountpoint.as_str()).map_err(|err| {
            warn!("Cannot umount, maybe it was not mounted: {err}");
            err
        });
    }

    let mount_point = mount::create_mount_point(
        Path::new(&mountpoint),
        matches.get_flag("allow-root"),
        matches.get_flag("allow-other"),
        matches.get_flag("direct-io"),
        matches.get_flag("suid"),
    );
    let mount_handle = mount_point.mount().await.map_err(|err| {
        error!(err = %err);
        ExitStatusError::Failure(1)
    })?;
    let mount_handle = Arc::new(Mutex::new(Some(Some(mount_handle))));
    let mount_handle_clone = mount_handle.clone();
    // cleanup on process kill
    set_handler(move || {
        // can't use tracing methods here as guard cannot be dropper to flush content before we exit
        eprintln!("Received signal to exit");
        let mut status: Option<ExitStatusError> = None;
        eprintln!("Unmounting {}", mountpoint);
        // create new tokio runtime
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _ = rt
            .block_on(async {
                let res = mount_handle_clone
                    .lock()
                    .await
                    .replace(None)
                    .unwrap()
                    .unwrap()
                    .umount()
                    .await;
                if res.is_err() {
                    umount(mountpoint.as_str())?;
                }
                Ok::<(), io::Error>(())
            })
            .map_err(|err| {
                eprintln!("Error: {}", err);
                status.replace(ExitStatusError::Failure(1));
                err
            });
        eprintln!("Bye!");
        process::exit(status.map_or(0, |x| match x {
            ExitStatusError::Failure(status) => status,
        }));
    })?;

    task::spawn_blocking(|| {
        let rt = tokio::runtime::Handle::current();
        rt.block_on(async {
            tokio::time::sleep(tokio::time::Duration::from_secs(u64::MAX)).await;
        })
    })
    .await?;

    Ok(())
}

fn umount(mountpoint: &str) -> io::Result<()> {
    // try normal umount
    if process::Command::new("umount")
        .arg(mountpoint)
        .output()?
        .status
        .success()
    {
        return Ok(());
    }
    // force umount
    if process::Command::new("umount")
        .arg("-f")
        .arg(mountpoint)
        .output()?
        .status
        .success()
    {
        return Ok(());
    }
    // lazy umount
    if process::Command::new("umount")
        .arg("-l")
        .arg(mountpoint)
        .output()?
        .status
        .success()
    {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Other,
            format!("cannot umount {}", mountpoint),
        )
        .into())
    }
}
