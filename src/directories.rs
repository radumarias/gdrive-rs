use crate::app_details::{APPLICATION, ORGANIZATION, QUALIFIER};
use crate::is_debug;
use directories::ProjectDirs;
use std::path::PathBuf;
use std::str::FromStr;
use std::{fs, panic};
use tracing::{error, instrument};

#[instrument]
pub fn get_project_dirs() -> ProjectDirs {
    let proj_dirs = if let Some(proj_dirs) = ProjectDirs::from(QUALIFIER, ORGANIZATION, APPLICATION)
    {
        proj_dirs
    } else {
        error!("Cannot get project directories");
        panic!("Cannot get project directories");
    };
    fs::create_dir_all(proj_dirs.config_dir()).expect("Cannot create config directory");
    fs::create_dir_all(proj_dirs.data_local_dir()).expect("Cannot create data directory");
    fs::create_dir_all(proj_dirs.data_local_dir().join("logs"))
        .expect("Cannot create logs directory");

    proj_dirs
}

#[allow(dead_code)]
pub fn get_config_dir() -> PathBuf {
    if is_debug() {
        get_dev_data_dir()
    } else {
        get_project_dirs().config_local_dir().to_path_buf()
    }
}

#[allow(dead_code)]
pub fn get_data_dir() -> PathBuf {
    if is_debug() {
        get_dev_data_dir()
    } else {
        get_project_dirs().data_local_dir().to_path_buf()
    }
}

#[allow(dead_code)]
pub fn get_logs_dir() -> PathBuf {
    if is_debug() {
        get_dev_data_dir()
    } else {
        get_project_dirs().data_local_dir().join("logs")
    }
}

fn get_dev_data_dir() -> PathBuf {
    let path = PathBuf::from_str(&format!(
        "/tmp/{}",
        APPLICATION.replace(" ", "-").to_lowercase()
    ))
    .unwrap();
    fs::create_dir_all(&path).expect("Cannot create data directory");
    path
}
