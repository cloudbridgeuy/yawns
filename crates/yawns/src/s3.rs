use crate::prelude::*;
use futures::future::join_all;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
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
    #[clap(name = "copy-list")]
    CopyList(CopyListOptions),

    /// Counts the number of objects in a bucket with a given prefix.
    #[clap(name = "count-files")]
    CountFiles(CountFilesOptions),
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
    aprintln!(
        "Copying from {} to {} with max_concurrent {}",
        options.source_bucket,
        options.destination_bucket,
        options.max_concurrent
    );

    let src = options.src.contents()?;
    let source_prefix = if let Some(source_prefix) = options.source_prefix.clone() {
        f!("{}/{}", options.source_bucket, source_prefix)
    } else {
        options.source_bucket.clone()
    };
    let destination_prefix = if let Some(destination_prefix) = options.destination_prefix.clone() {
        f!("{}/", destination_prefix)
    } else {
        "/".to_string()
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
            aprintln!(
                "Progress: {}/{} files copied in {:.2} seconds ({:.2} files/second)",
                count,
                document_lines_length,
                elapsed.as_secs_f64(),
                rate
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

    let copy_futures = document_lines.map(|line| {
        let request = request.clone();

        let source_key = f!("{}/{}", source_prefix, line);
        // INFO: Notice the lack of `/` in the `f!` call!
        //       This is because we've set the prefix outside the
        //       creation of this future, when creating the
        //       `destination_prefix` variable.
        let destination_key = f!("{}{}", destination_prefix, line);
        let document_lines_length = document_lines_length.clone();

        let copied_count = copied_count.clone();
        let semaphore = semaphore.clone();

        async move {
            // Acquire a permit for the semaphore
            let _permit = semaphore.acquire().await.unwrap();

            let response = request
                .copy_source(&source_key)
                .key(destination_key.as_str())
                .send()
                .await?;

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
