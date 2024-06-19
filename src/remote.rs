use google_drive3::hyper::client::HttpConnector;
use google_drive3::hyper_rustls::HttpsConnector;
use google_drive3::DriveHub;

pub(crate) mod wrapper;

pub(crate) trait RemoteFilesystem {
    fn get_hub(&self) -> DriveHub<HttpsConnector<HttpConnector>>;
}
