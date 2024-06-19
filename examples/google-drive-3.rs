extern crate google_drive3 as drive3;

use std::default::Default;
use std::fs::File;
use std::str::FromStr;

use drive3::Error;
use drive3::{hyper, hyper_rustls, oauth2, DriveHub};

use anyhow::Result;
use drive3::hyper::client::HttpConnector;
use drive3::hyper_rustls::HttpsConnector;
use md5::digest::typenum::private::IsEqualPrivate;

use serde_json::Value;

#[tokio::main]
async fn main() -> Result<()> {
    // Get an ApplicationSecret instance by some means. It contains the `client_id` and
    // `client_secret`, among other things.
    let secrets_josn = include_str!("../../client-secret.json");
    let secret: oauth2::ApplicationSecret = serde_json::from_str(secrets_josn)?;
    // Instantiate the authenticator. It will choose a suitable authentication flow for you,
    // unless you replace `None` with the desired Flow.
    // Provide your own `AuthenticatorDelegate` to adjust the way it operates and get feedback about
    // what's going on. You probably want to bring in your own `TokenStorage` to persist tokens and
    // retrieve them from storage.
    let auth = oauth2::InstalledFlowAuthenticator::builder(
        secret,
        oauth2::InstalledFlowReturnMethod::HTTPRedirect,
    )
    .persist_tokens_to_disk("token_store.json")
    .build()
    .await
    .unwrap();
    let hub: DriveHub<HttpsConnector<HttpConnector>> = DriveHub::new(
        hyper::Client::builder().build(
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_native_roots()
                .https_or_http()
                .enable_http1()
                .build(),
        ),
        auth,
    );
    // You can configure optional parameters by calling the respective setters at will, and
    // execute the final call using `doit()`.
    // Values shown here are possibly random and not representative !
    //     let result = hub.files().list()
    //         .team_drive_id("et")
    //         .supports_team_drives(true)
    //         .supports_all_drives(false)
    //         .spaces("amet.")
    //         .q("takimata")
    //         .page_token("amet.")
    //         .page_size(-20)
    //         .order_by("ipsum")
    //         .include_team_drive_items(true)
    //         .include_permissions_for_view("Lorem")
    //         .include_labels("gubergren")
    //         .include_items_from_all_drives(false)
    //         .drive_id("dolor")
    //         .corpus("ea")
    //         .corpora("ipsum")
    //         .doit().await;
    let result = hub
        .files()
        .list()
        .q("'root' in parents and trashed = false")
        .param(
            "fields",
            "files(id,name,kind,size,parents,createdTime,modifiedTime,ownedByMe,mimeType)",
        )
        .include_items_from_all_drives(false)
        .doit()
        .await;

    match result {
        Err(e) => match e {
            // The Error enum provides details about what exactly happened.
            // You can also just use its `Debug`, `Display` or `Error` traits
            Error::HttpError(_)
            | Error::Io(_)
            | Error::MissingAPIKey
            | Error::MissingToken(_)
            | Error::Cancelled
            | Error::UploadSizeLimitExceeded(_, _)
            | Error::Failure(_)
            | Error::BadRequest(_)
            | Error::FieldClash(_)
            | Error::JsonDecodeError(_, _) => println!("{}", e),
        },
        Ok(res) => {
            // println!("Success: {:?}", res);
            for f in res.1.files.unwrap() {
                if let Some(owned_by_me) = f.owned_by_me {
                    if !owned_by_me {
                        continue;
                    }
                }
                // println!("{}", serde_json::to_string(&f)?);
                // Serialize to serde_json::Value
                let mut value = serde_json::to_value(&f).unwrap();

                // Remove fields that are None
                if let Value::Object(ref mut map) = value {
                    map.retain(|_, v| !v.is_null());
                }

                // Serialize the modified value to a JSON string
                let json = serde_json::to_string(&value).unwrap();
                println!("{}", json);
                // println!("File: id {:?} name {:?} kind {:?} size {:?} parents {:?} created_time {:?} modified_time {:?} file_extension {:?} full_file_extension {:?} mime_type {:?}",
                //          f.id, f.name, f.kind, f.size, f.parents, f.created_time, f.modified_time, f.file_extension, f.full_file_extension, f.mime_type);
            }
        }
    }

    // let f = google_drive3::api::File {
    //     name: Some("token_store.json".to_string()),
    //     kind: Some("drive#file".to_string()),
    //     mime_type: Some("application/json".to_string()),
    //     ..Default::default()
    // };
    // hub.files()
    //     .create(f)
    //     .upload_resumable(
    //         File::open("token_store.json")?,
    //         mime::Mime::from_str("application/json")?,
    //     )
    //     .await?;

    Ok(())
}
