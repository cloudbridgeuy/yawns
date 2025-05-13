use crate::prelude::*;
use aws_smithy_types::byte_stream::ByteStream;
use futures::future::join_all;
use std::path::PathBuf;
use std::str::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::Semaphore;
use tokio::time::{sleep, Duration};

#[derive(Debug, clap::Parser)]
#[command(name = "s3")]
#[command(about = "Amazon S3 (Amazon Simple Storage Service)")]
pub struct App {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, clap::Subcommand)]
pub enum Commands {
    /// Get the list of Buckets under the given account.
    #[clap(name = "list-buckets")]
    ListBuckets,

    /// Copies an object between buckets.
    #[clap(name = "copy")]
    Copy(CopyOptions),

    /// Copies a list of objects between buckets.
    ///
    /// The list of files to copy can be given as a CSV file with at least three columns:
    /// file, source_prefix, destination_prefix, and optionally, metadata. Each column
    /// should be separated by a comma. Metadata key value pairs are defined as `key=value`
    /// strings separated by a space.
    #[clap(name = "copy-list")]
    CopyList(CopyListOptions),

    /// Counts the number of objects in a bucket with a given prefix.
    #[clap(name = "count-files")]
    CountFiles(CountFilesOptions),

    /// Uploads a list of local objects to a remote Bucket.
    ///
    /// The list of files to copy can be given as a CSV file with at least three columns:
    /// file, source_prefix, destination_prefix, and optionally, metadata. Each column
    /// should be separated by a comma. Metadata key value pairs are defined as `key=value`
    /// strings separated by a space.
    #[clap(name = "upload-list")]
    UploadList(UploadListOptions),
}

#[derive(Debug, clap::Args, serde::Serialize, serde::Deserialize, Clone)]
pub struct CopyOptions {
    /// AWS S3 Source Bucket.
    #[clap(long, env = "AWS_S3_SRC_BUCKET")]
    source_bucket: String,
    /// AWS S3 Destination Bucket.
    #[clap(long, env = "AWS_S3_DST_BUCKET")]
    destination_bucket: String,
    /// AWS S3 Source Object.
    #[clap(env = "AWS_S3_SRC_OBJECT")]
    src: String,
    /// AWS S3 Destination Object.
    #[clap(env = "AWS_S3_DST_OBJECT")]
    dst: String,
}

#[derive(Debug, clap::Args, Clone)]
pub struct CopyListOptions {
    /// AWS S3 Source Bucket.
    #[clap(long, env = "AWS_S3_SRC_BUCKET")]
    source_bucket: String,
    /// AWS S3 Destination Bucket.
    #[clap(long, env = "AWS_S3_DST_BUCKET")]
    destination_bucket: String,
    /// AWS S3 Source Object list read from file or Stdin (default.)
    #[clap(env = "AWS_S3_SRC_OBJECT_LIST", default_value = "-")]
    src: clap_stdin::FileOrStdin,
    /// AWS S3 Source Object prefix.
    #[clap(long, env = "AWS_S3_SRC_OBJECT_PREFIX")]
    source_prefix: Option<String>,
    /// AWS S3 Source Object prefix.
    #[clap(long, env = "AWS_S3_DST_OBJECT_PREFIX")]
    destination_prefix: Option<String>,
    /// Max concurrent copy threads to control the copy rate.
    #[clap(long, env = "AWS_S3_MAX_CONCURRENT", default_value = "10")]
    max_concurrent: usize,
    /// Metadata to add to the copied object in the form of KEY=VALUE pairs.
    #[clap(short, long, value_parser = parse_key_val::<String, String>, number_of_values = 1)]
    metadata: Option<Vec<(String, String)>>,
}

#[derive(Debug, clap::Args, Clone)]
pub struct UploadListOptions {
    /// List of local files to upload and their destination details read from file or Stdin (default.)
    /// Each line should be in the format: local_path,destination_prefix[,metadata_key1=value1 metadata_key2=value2...]
    /// Metadata is optional and space-separated key=value pairs.
    #[clap(env = "AWS_S3_SRC_OBJECT_LIST", default_value = "-")]
    src: clap_stdin::FileOrStdin,
    /// AWS S3 Destination Bucket.
    #[clap(long, env = "AWS_S3_DST_BUCKET")]
    destination_bucket: String,
    /// AWS S3 Source Object prefix.
    #[clap(long, env = "AWS_S3_DST_OBJECT_PREFIX")]
    destination_prefix: Option<String>,
    /// Max concurrent upload threads to control the upload rate.
    #[clap(long, env = "AWS_S3_MAX_CONCURRENT", default_value = "10")]
    max_concurrent: usize,
}

#[derive(Debug, clap::Args, Clone)]
pub struct CountFilesOptions {
    /// AWS S3 Bucket.
    #[clap(long, env = "AWS_S3_BUCKET")]
    bucket: String,
    /// AWS S3 Object prefix to count.
    #[clap(long, env = "AWS_S3_OBJECT_PREFIX")]
    prefix: Option<String>,
}

/// Parse a single key-value pair
fn parse_key_val<T, U>(
    s: &str,
) -> Result<(T, U), Box<dyn std::error::Error + Send + Sync + 'static>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
    U: std::str::FromStr,
    U::Err: std::error::Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

pub async fn run(app: App, global: crate::Global) -> Result<()> {
    if global.verbose {
        aprintln!("S3 Client Version: {}", aws_sdk_s3::meta::PKG_VERSION);
        aprintln!(
            "AWS Region        : {}",
            global
                .region
                .as_ref()
                .ok_or_else(|| eyre!("AWS_REGION not defined"))?
        );
        aprintln!();
    }

    let config = crate::aws::get_sdk_config_from_global(global).await?;
    let client = aws_sdk_s3::Client::new(&config);

    match app.command {
        Commands::ListBuckets => list_buckets(client).await,
        Commands::Copy(options) => copy(client, options).await,
        Commands::CopyList(options) => copy_list(client, options).await,
        Commands::CountFiles(options) => count_files(client, options).await,
        Commands::UploadList(options) => upload_list(client, options).await,
    }
}

pub async fn list_buckets(client: aws_sdk_s3::Client) -> Result<()> {
    let resp = client.list_buckets().send().await?;

    log::info!("Getting the list of Buckets");
    let buckets = resp.buckets.ok_or_eyre("No buckets found")?;

    let mut table = new_table();
    table.set_titles(prettytable::row!["Name", "CreatedAt"]);

    for bucket in buckets {
        table.add_row(prettytable::row![
            bucket.name.ok_or_eyre("No name")?,
            bucket.creation_date.ok_or_eyre("No creation date")?
        ]);
    }

    aprintln!("{}", table.to_string());

    Ok(())
}

/// Copy an object from one bucket to another.
pub async fn copy(client: aws_sdk_s3::Client, options: CopyOptions) -> Result<()> {
    let source_key = f!("{}/{}", options.source_bucket, options.src);
    let response = client
        .copy_object()
        .copy_source(&source_key)
        .bucket(options.destination_bucket.as_str())
        .key(options.dst.as_str())
        .send()
        .await?;

    aprintln!(
        "Copied from {source_key} to {}/{} with etag {}",
        options.destination_bucket,
        options.dst,
        response
            .copy_object_result
            .ok_or_eyre("CopyObjectResult not found")?
            .e_tag()
            .ok_or_eyre("ETag not found")?
    );
    Ok(())
}

/// Copy a list of objects from one bucket to another.
pub async fn copy_list(client: aws_sdk_s3::Client, options: CopyListOptions) -> Result<()> {
    let src = options.src.contents()?;
    let source_prefix = if let Some(source_prefix) = options.source_prefix.clone() {
        f!("{}/{}", options.source_bucket, source_prefix)
    } else {
        options.source_bucket.clone()
    };
    let destination_prefix = if let Some(destination_prefix) = options.destination_prefix.clone() {
        destination_prefix
    } else {
        "".to_string()
    };
    let metadata = options.metadata.unwrap_or_default();

    // Atomic counter for tracking copied files
    let copied_count = Arc::new(AtomicUsize::new(0));
    let start_time = Instant::now();

    // Create a semaphore to control concurrency
    let semaphore = Arc::new(Semaphore::new(options.max_concurrent));

    let document_lines = src.split("\n");
    let document_lines_length = document_lines.clone().count();

    // Spawn a progress logger task in a separate async task
    let copied_count_for_progress = copied_count.clone();
    let progress_handle = tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(5)).await; // Update every 5 seconds
            let count = copied_count_for_progress.load(Ordering::Relaxed);
            let elapsed = start_time.elapsed();
            let rate = if elapsed.as_secs_f64() > 0.0 {
                count as f64 / elapsed.as_secs_f64()
            } else {
                0.0
            };
            let time_remaining = if rate > 0.0 {
                (document_lines_length - count) as f64 / rate
            } else {
                0.0
            };
            aprintln!(
                "Progress: {}/{} files copied in {:.2} seconds ({:.2} files/second) time remaining {:.2} seconds",
                count,
                document_lines_length,
                elapsed.as_secs_f64(),
                rate,
                time_remaining
            );
        }
    });

    let mut request = client
        .copy_object()
        .bucket(options.destination_bucket.clone())
        .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace);

    for (key, value) in metadata {
        request = request.metadata(key, value);
    }

    aprintln!(
        "Copying files from bucket {} to bucket {}",
        options.source_bucket,
        options.destination_bucket
    );

    let copy_futures = document_lines.map(|line| {
        let mut request = request.clone();
        let destination_bucket = options.destination_bucket.clone();
        let source_bucket = options.source_bucket.clone();

        // Parse the `line` as if it was a `CSV` line with columns: `file`, `source_prefix`, and
        // `destination_prefix`.
        let tuple = line.split(",").collect::<Vec<_>>();

        if tuple.len() < 3 {
            panic!(
                "Invalid line format: {}. Expected at least 3 columns.",
                line
            );
        }

        let file = tuple[0];
        let source_prefix = f!("{}/{}", source_bucket, tuple[1]);
        let destination_prefix = tuple[2];

        if tuple.len() == 4 {
            let serialized_metadata = tuple[3];
            let serialized_pairs = serialized_metadata.split(" ").collect::<Vec<_>>();
            for pair in serialized_pairs {
                let split_vec: Vec<&str> = pair.split("=").collect::<Vec<_>>();
                if split_vec.len() != 2 {
                    continue;
                }
                request = request.metadata(split_vec[0], split_vec[1]);
            }
        }

        let source_key = f!("{}{}", source_prefix, file);
        let destination_key = f!("{}{}", destination_prefix, file);

        let copied_count = copied_count.clone();
        let semaphore = semaphore.clone();

        async move {
            // Acquire a permit for the semaphore
            let _permit = semaphore.acquire().await.unwrap();

            let response = match request
                .copy_source(&source_key)
                .key(destination_key.as_str())
                .send()
                .await
            {
                Ok(response) => response,
                Err(err) => {
                    aprintln!(
                        "Failed to copy from {source_key} to {destination_key}. Error: {}",
                        err
                    );
                    return Ok(());
                }
            };

            if let Some(copy_object_result) = response.copy_object_result {
                if copy_object_result.e_tag.is_none() {
                    aprintln!("Failed to copy from {source_key}: No ETag found",);
                }
            } else {
                aprintln!("Failed to copy from {source_key}: No CopyObjectResult found",);
            }

            copied_count.fetch_add(1, Ordering::Relaxed);

            Ok(()) as Result<()>
        }
    });

    _ = join_all(copy_futures).await;

    // Cancel the progress task when all copy operations are complete
    progress_handle.abort();

    let total_copied = copied_count.load(Ordering::Relaxed);
    let duration = start_time.elapsed();
    let rate = total_copied as f64 / duration.as_secs_f64();

    aprintln!(
        "\nCopied {}/{} files in {:.2} seconds ({:.2} files/second)",
        total_copied,
        document_lines_length,
        duration.as_secs_f64(),
        rate
    );

    Ok(())
}

/// Counts the number of objects in a bucket with a given prefix.
pub async fn count_files(client: aws_sdk_s3::Client, options: CountFilesOptions) -> Result<()> {
    aprintln!(
        "Counting files in bucket: {} with prefix: {}",
        options.bucket,
        options.prefix.as_deref().unwrap_or("(none)")
    );

    let mut object_count: u64 = 0;
    let mut continuation_token: Option<String> = None;

    loop {
        let mut list_objects_req = client.list_objects_v2().bucket(options.bucket.as_str());

        if let Some(prefix) = options.prefix.as_deref() {
            list_objects_req = list_objects_req.prefix(prefix);
        }

        if let Some(token) = continuation_token {
            list_objects_req = list_objects_req.continuation_token(token);
        }

        let resp = list_objects_req.send().await?;

        if let Some(contents) = resp.contents {
            object_count += contents.len() as u64;
        }

        if let Some(next_token) = resp.next_continuation_token {
            continuation_token = Some(next_token);
        } else {
            break; // No more pages
        }
    }

    aprintln!("Total objects counted: {}", object_count);

    Ok(())
}

/// Upload a list of local files to an S3 bucket.
pub async fn upload_list(client: aws_sdk_s3::Client, options: UploadListOptions) -> Result<()> {
    let src_contents = options.src.contents()?;

    // Atomic counter for tracking uploaded files
    let uploaded_count = Arc::new(AtomicUsize::new(0));
    let failed_count = Arc::new(AtomicUsize::new(0));
    let start_time = Instant::now();

    // Create a semaphore to control concurrency
    let semaphore = Arc::new(Semaphore::new(options.max_concurrent));

    let document_lines: Vec<_> = src_contents
        .lines()
        .filter(|l| !l.trim().is_empty())
        .collect();
    let document_lines_length = document_lines.len();

    // Spawn a progress logger task
    let uploaded_count_for_progress = uploaded_count.clone();
    let failed_count_for_progress = failed_count.clone();
    let progress_handle = tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(5)).await; // Update every 5 seconds
            let uploaded = uploaded_count_for_progress.load(Ordering::Relaxed);
            let failed = failed_count_for_progress.load(Ordering::Relaxed);
            let elapsed = start_time.elapsed();
            let total_processed = uploaded + failed;
            let rate = if elapsed.as_secs_f64() > 0.0 {
                total_processed as f64 / elapsed.as_secs_f64()
            } else {
                0.0
            };
            let time_remaining = if rate > 0.0 {
                (document_lines_length - total_processed) as f64 / rate
            } else {
                0.0
            };
            aprintln!(
                "Progress: {}/{} files uploaded, {} failed in {:.2} seconds ({:.2} files/second) time remaining {:.2} seconds",
                uploaded,
                document_lines_length,
                failed,
                elapsed.as_secs_f64(),
                rate,
                time_remaining
            );
        }
    });

    aprintln!("Uploading files to bucket {}", options.destination_bucket);

    let upload_futures = document_lines.into_iter().map(|line| {
        let client = client.clone();
        let destination_bucket = options.destination_bucket.clone();
        let destination_prefix = options.destination_prefix.clone().unwrap_or_default();
        let uploaded_count = uploaded_count.clone();
        let failed_count = failed_count.clone();
        let semaphore = semaphore.clone();

        async move {
            let tuple: Vec<&str> = line.split(',').collect();

            if tuple.is_empty() {
                aprintln!("Invalid line format: `{}`. Expected at least 1 or 2 columns (local_path, [destination_prefix]).", line);
                failed_count.fetch_add(1, Ordering::Relaxed);
                return; // Skip invalid line
            }

            let local_path_str = tuple[0].trim();
            let destination_prefix_str = if tuple.len() == 2 { tuple[1] } else { &destination_prefix };
            let metadata_str = tuple.get(2).map(|s| s.trim()).unwrap_or("");

            let local_path = PathBuf::from(local_path_str);
            let file_name = match local_path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name,
                None => {
                    aprintln!("Invalid local path: `{}`. Cannot extract file name.", local_path_str);
                    failed_count.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };

            let s3_key = if destination_prefix_str.is_empty() {
                file_name.to_string()
            } else {
                // Ensure no double slash if prefix doesn't end with one
                if destination_prefix_str.ends_with('/') {
                     f!("{}{}", destination_prefix_str, file_name)
                } else {
                     f!("{}/{}", destination_prefix_str, file_name)
                }
            };

            let mut metadata: std::collections::HashMap<String, String> = std::collections::HashMap::new();
            if !metadata_str.is_empty() {
                 let pairs = metadata_str.split_whitespace();
                 for pair in pairs {
                     let split_pair: Vec<&str> = pair.splitn(2, '=').collect();
                     if split_pair.len() == 2 {
                         metadata.insert(split_pair[0].to_string(), split_pair[1].to_string());
                     } else {
                         aprintln!("Warning: Invalid metadata pair format in line `{}`: `{}`. Expected key=value.", line, pair);
                     }
                 }
            }

            // Acquire a permit for the semaphore
            let _permit = match semaphore.acquire().await {
                 Ok(p) => p,
                 Err(e) => {
                     aprintln!("Failed to acquire semaphore permit: {}. Skipping upload for {}", e, local_path_str);
                     failed_count.fetch_add(1, Ordering::Relaxed);
                     return;
                 }
            };

            let upload_result = async {
                 // Read file content
                 let mut file = File::open(&local_path).await.map_err(|e| eyre!("Failed to open file {}: {}", local_path_str, e))?;
                 let mut contents = Vec::new();
                 file.read_to_end(&mut contents).await.map_err(|e| eyre!("Failed to read file {}: {}", local_path_str, e))?;
                 let body = ByteStream::from_path(&local_path).await?;

                 // Build PutObject request
                 let mut request = client
                     .put_object()
                     .bucket(destination_bucket.as_str())
                     .key(s3_key.as_str())
                     .body(body);

                 for (key, value) in metadata {
                     request = request.metadata(key, value);
                 }

                 // Send request
                 request.send().await.map_err(|e| eyre!("S3 PutObject failed for {}: {}", local_path_str, e))?;

                 Ok(()) as Result<()>
            }
            .await;

            match upload_result {
                Ok(_) => {
                    uploaded_count.fetch_add(1, Ordering::Relaxed);
                    // Optionally log success
                    // aprintln!("Uploaded {} to {}/{}", local_path_str, destination_bucket, s3_key);
                }
                Err(e) => {
                    aprintln!("Failed to upload {}: {}", local_path_str, e);
                    failed_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    });

    join_all(upload_futures).await;

    // Cancel the progress task when all upload operations are complete
    progress_handle.abort();

    let total_uploaded = uploaded_count.load(Ordering::Relaxed);
    let total_failed = failed_count.load(Ordering::Relaxed);
    let duration = start_time.elapsed();
    let rate = (total_uploaded + total_failed) as f64 / duration.as_secs_f64();

    aprintln!(
        "\nUpload Summary: {}/{} files uploaded, {} failed in {:.2} seconds ({:.2} files/second)",
        total_uploaded,
        document_lines_length,
        total_failed,
        duration.as_secs_f64(),
        rate
    );

    if total_failed > 0 {
        Err(eyre!("{} file(s) failed to upload.", total_failed))
    } else {
        Ok(())
    }
}
