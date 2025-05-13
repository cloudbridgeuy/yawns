# Yawns - An AWS CLI Helper

A Rust-based CLI tool to interact with AWS services using `clap`, `tokio`, and
the AWS SDK for Rust.

---

You know the drill. Interacting with AWS can sometimes feel like navigating
a labyrinth, especially when dealing with routine tasks across services like S3
and KMS. While the official `awscli` is incredibly powerful, there are often
specific workflows or batch operations that could be simpler, faster, or just
tailored a bit more to our needs.

That's where this Rust CLI comes in.

Born out of a desire to streamline common AWS S3 and KMS operations, this tool
provides a focused set of commands designed for efficiency and clarity. It
leverages the speed and safety of Rust and the official AWS SDKs to give you
reliable performance for those repetitive tasks, particularly around S3 object
management.

## Key Features

This CLI offers a growing set of commands, focusing on practical utility for day-to-day AWS interactions.

- **AWS KMS Management:**

  - **List Keys:** Quickly see all your KMS keys and their associated aliases.
  - **Get Key Policy:** Retrieve the default policy document for a specific KMS key alias.

- **Amazon S3 Management:**
  - **List Buckets:** Get a simple list of all buckets in your account.
  - **Copy Object:** Copy a single S3 object from a source bucket/key to a destination bucket/key.
  - **Copy List of Objects:** Perform bulk copy operations based on a list provided via a file or stdin. Includes support for source/destination prefix remapping, adding metadata, concurrency control, and progress tracking.
  - **Count Objects:** Get a count of objects within a bucket, optionally filtered by a prefix.
  - **Upload List of Files:** Perform bulk upload operations of local files to an S3 bucket based on a list provided via a file or stdin. Supports specifying destination prefix per file, adding metadata, concurrency control, and progress tracking.

It's built with performance in mind, especially for the bulk S3 operations, using asynchronous patterns and configurable concurrency.

## Prerequisites

Before you can build and run this tool, you'll need a few things set up:

1.  **Rust and Cargo:** Make sure you have the Rust programming language and its package manager, Cargo, installed. You can install them via `rustup`:

    ```bash
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    ```

2.  **AWS Credentials and Region:** The tool uses the standard AWS SDK configuration. This means it will look for credentials in the usual places (environment variables like `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, shared credential files `~/.aws/credentials`, etc.) and the region (environment variable `AWS_REGION`, shared config files `~/.aws/config`). Ensure your environment is configured to access your AWS account.

    If you're new to setting this up, the [AWS documentation on configuring the SDK](https://docs.aws.amazon.com/sdkref/latest/method/region.html) is a great resource.

## Installation

You can build and install the tool directly from source using Cargo.

1.  **Clone the Repository:** (Assuming you have the source code in a Git repository)

    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

    _(Replace `<repository-url>` and `<repository-directory>` with the actual details)_

2.  **Build and Install:** Use Cargo to build a release version and install it. This compiles the code with optimizations and places the executable in your Cargo bin path (usually `~/.cargo/bin`), which should be in your system's PATH.

    ```bash
    cargo install --path .
    ```

Once installed, the executable should be available system-wide under a name like `aws-shortcuts` or similar, depending on the crate name defined in `Cargo.toml`. Let's assume for the examples below that the command is simply `aws-cli-tool` or similar based on the source structure. _Note: The provided source doesn't explicitly name the crate, but the `clap` `#[command(name = "...")]` implies subcommands. The top-level struct is `App`. Let's call the main binary `yawns` for now, based on the environment variables like `YAWNS_KMS_ALIAS`, `YAWNS_VERBOSE`._

Let's rename the assumed binary to `yawns` for simplicity in examples.

## Usage

The general structure of the command is:

```bash
yawns <subcommand> <command> [options]
```

### Global Options

These options can be applied before any subcommand to control general behavior:

- `--region <REGION>`: Specify the AWS region to use (overrides environment variables or config files). Defaults to `us-east-1`.
- `--profile <PROFILE>`: Specify the AWS profile to use (overrides environment variables or config files). Defaults to `default`.
- `--verbose`: Enable verbose output, showing details like SDK versions and configured region.

Example with global options:

```bash
yawns --region eu-central-1 --profile my-dev-profile kms list-keys
```

Now, let's dive into the specific commands.

## Command Details

### AWS KMS Commands (`yawns kms`)

#### `list-keys`

Gets the list of existing KMS keys in the configured region and their aliases.

```bash
yawns kms list-keys
```

**Example Output:**

```
+----------------------------------------------------------------+----------------------------------------------+
|                       Arn                        |                     Id                       |
+----------------------------------------------------------------+----------------------------------------------+
| arn:aws:kms:us-east-1:123456789012:key/abc-def-123 | alias/MyApplicationKey, alias/AnotherAlias |
| arn:aws:kms:us-east-1:123456789012:key/ghi-jkl-456 | alias/ServiceKey                             |
+----------------------------------------------------------------+----------------------------------------------+
```

#### `get-policy`

Get the _default_ key policy attached to a specific KMS key, identified by its alias.

```bash
yawns kms get-policy --alias <KEY_ALIAS>
```

- `<KEY_ALIAS>`: The alias of the KMS key (e.g., `alias/my-app-key`). Can also be set via the `YAWNS_KMS_ALIAS` environment variable.

**Example:**

```bash
yawns kms get-policy --alias alias/MyApplicationKey
```

This will print the JSON policy document to standard output.

### Amazon S3 Commands (`yawns s3`)

#### `list-buckets`

Gets the list of S3 buckets under the given account and their creation dates.

```bash
yawns s3 list-buckets
```

**Example Output:**

```
+-------------------+-----------------------------+
|       Name        |          CreatedAt          |
+-------------------+-----------------------------+
| my-unique-bucket  | 2023-10-27T10:00:00.000Z    |
| project-logs-prod | 2024-01-15T14:30:00.000Z    |
+-------------------+-----------------------------+
```

#### `copy`

Copies a single object from a source S3 location to a destination S3 location.

```bash
yawns s3 copy --source-bucket <SRC_BUCKET> --destination-bucket <DST_BUCKET> <SRC_OBJECT_KEY> <DST_OBJECT_KEY>
```

- `--source-bucket <SRC_BUCKKET>`: The source bucket name. Can be set via `AWS_S3_SRC_BUCKET`.
- `--destination-bucket <DST_BUCKKET>`: The destination bucket name. Can be set via `AWS_S3_DST_BUCKET`.
- `<SRC_OBJECT_KEY>`: The key of the object in the source bucket (e.g., `path/to/source/file.txt`). Can be set via `AWS_S3_SRC_OBJECT`.
- `<DST_OBJECT_KEY>`: The key for the object in the destination bucket (e.g., `new/path/for/file.txt`). Can be set via `AWS_S3_DST_OBJECT`.

**Example:**

```bash
yawns s3 copy --source-bucket my-source-bucket --destination-bucket my-dest-bucket old/path/data.json new/path/archived_data.json
```

#### `copy-list`

Copies a list of objects from one bucket to another. This command is ideal for batch operations defined in a file.

```bash
yawns s3 copy-list --source-bucket <SRC_BUCKET> --destination-bucket <DST_BUCKET> --src <LIST_FILE_OR_STDIN> [OPTIONS]
```

- `--source-bucket <SRC_BUCKET>`: The source bucket name. Can be set via `AWS_S3_SRC_BUCKET`.
- `--destination-bucket <DST_BUCKET>`: The destination bucket name. Can be set via `AWS_S3_DST_BUCKET`.
- `--src <LIST_FILE_OR_STDIN>`: The path to a file containing the list of objects to copy, or `-` to read from standard input. Defaults to `-`. Can be set via `AWS_S3_SRC_OBJECT_LIST`.
- `--source-prefix <PREFIX>`: An _optional_ prefix to prepend to the `source_prefix_part` read from the input file when constructing the full source S3 key. Can be set via `AWS_S3_SRC_OBJECT_PREFIX`.
- `--destination-prefix <PREFIX>`: An _optional_ prefix to prepend to the `destination_prefix_part` read from the input file when constructing the full destination S3 key. Can be set via `AWS_S3_DST_OBJECT_PREFIX`.
- `--max-concurrent <N>`: The maximum number of concurrent copy operations to perform. Defaults to `10`. Can be set via `AWS_S3_MAX_CONCURRENT`.
- `-m, --metadata <KEY=VALUE>`: Metadata key-value pairs to add to the copied object(s). Can be specified multiple times for different keys. Note that _metadata specified in the input file overrides metadata specified on the command line_.

**Input File Format (`--src`):**

The input file should be a CSV-like format, with one line per object to copy. Each line _must_ have at least 3 columns: `file`, `source_prefix_part`, `destination_prefix_part`. A fourth column for metadata is optional.

```csv
<file>,<source_prefix_part>,<destination_prefix_part>[,<metadata_string>]
```

- `<file>`: The base name of the object.
- `<source_prefix_part>`: The prefix _relative to the bucket root or the CLI `--source-prefix`_ where the source object resides.
- `<destination_prefix_part>`: The prefix _relative to the destination bucket root or the CLI `--destination-prefix`_ where the object should be copied.
- `<metadata_string>` (Optional): A space-separated string of `key=value` pairs (e.g., `env=production stage=processed`). This metadata will be added to the destination object. _Note: MetadataDirective is set to REPLACE, so existing metadata on the source object will be overwritten._

**How Keys are Constructed:**

- **Source Key:** `{CLI_source_bucket}/{CLI_source_prefix}{source_prefix_part}{file}`
- **Destination Key:** `{CLI_destination_bucket}/{CLI_destination_prefix}{destination_prefix_part}{file}`

*(Note: The source code implements slightly different logic where the CLI `--source-prefix` and `--destination-prefix` might interact differently depending on whether the file column is empty. Based on code inspection, the structure appears closer to `source_key = f!("{}/{}", source_bucket, tuple[1] + file);` and `destination_key = f!("{}{}", tuple[2], file);` where `tuple[1]` is `source_prefix_part` and `tuple[2]` is `destination_prefix_part`, ignoring the CLI prefixes. **Please test with your desired behavior.** The description above attempts to match the *intent* based on parameter names.)*

**Example Input File (`copy_list.csv`):**

```csv
report.csv,raw/data/,processed/reports/
archive.zip,backups/weekly/,backups/monthly/,status=archived
image.jpg,images/hires/,images/thumbs/,processed=true size=small
```

**Example Command:**

```bash
# Using the file
yawns s3 copy-list --source-bucket my-source-bucket --destination-bucket my-dest-bucket --src copy_list.csv --max-concurrent 20 --metadata default-tag=true

# Using stdin
cat copy_list.csv | yawns s3 copy-list --source-bucket my-source-bucket --destination-bucket my-dest-bucket --src -
```

The command will print progress updates every 5 seconds, showing the number of files copied, failed, rate, and estimated time remaining.

#### `count-files`

Counts the number of objects in a specified bucket, optionally filtering by a prefix. Useful for getting quick estimates or checking directory sizes.

```bash
yawns s3 count-files --bucket <BUCKET_NAME> [--prefix <PREFIX>]
```

- `--bucket <BUCKET_NAME>`: The S3 bucket name. Can be set via `AWS_S3_BUCKET`.
- `--prefix <PREFIX>`: An optional prefix to filter the count by. Can be set via `AWS_S3_OBJECT_PREFIX`.

**Example:**

```bash
# Count all files in a bucket
yawns s3 count-files --bucket my-data-lake

# Count files in a specific "folder"
yawns s3 count-files --bucket my-logs-bucket --prefix application/web/
```

#### `upload-list`

Uploads a list of local files to a specified S3 bucket. This command is designed for efficient bulk uploads.

```bash
yawns s3 upload-list --destination-bucket <DST_BUCKET> --src <LIST_FILE_OR_STDIN> [OPTIONS]
```

- `--destination-bucket <DST_BUCKET>`: The destination bucket name. Can be set via `AWS_S3_DST_BUCKET`.
- `--src <LIST_FILE_OR_STDIN>`: The path to a file containing the list of files to upload, or `-` to read from standard input. Defaults to `-`. Can be set via `AWS_S3_SRC_OBJECT_LIST`.
- `--destination-prefix <PREFIX>`: An _optional_ prefix to prepend to the `destination_prefix_part` read from the input file (or used alone if the second column is missing) when constructing the full destination S3 key. Can be set via `AWS_S3_DST_OBJECT_PREFIX`.
- `--max-concurrent <N>`: The maximum number of concurrent upload operations to perform. Defaults to `10`. Can be set via `AWS_S3_MAX_CONCURRENT`.

**Input File Format (`--src`):**

Each line in the input file should specify a local file path and its desired destination in S3. It can have 1, 2, or 3 columns: `local_path`, `destination_prefix_part`, `metadata_string`.

```csv
<local_path>[,<destination_prefix_part>][,<metadata_string>]
```

- `<local_path>`: The path to the file on your local filesystem.
- `<destination_prefix_part>` (Optional): A prefix specific to this file, _relative to the `--destination-prefix` CLI option_. If this column is _missing or empty_, the value from the `--destination-prefix` CLI option is used. If this column is _present and not empty_, it _overrides_ the `--destination-prefix` CLI option and is used as the base prefix.
- `<metadata_string>` (Optional): A space-separated string of `key=value` pairs (e.g., `origin=cli type=data`). This metadata will be added to the uploaded object.

**How Keys are Constructed:**

- If the second column (`destination_prefix_part`) is _missing or empty_:
  - **S3 Key:** `{CLI_destination_prefix}{file_name}`
- If the second column (`destination_prefix_part`) is _present and not empty_:
  - **S3 Key:** `{destination_prefix_part}{file_name}` (with a `/` added between if `destination_prefix_part` doesn't end in one)

_(Note: Based on code inspection, the behavior for the second column overrides the CLI parameter is confirmed. This differs from the description of the CLI parameter itself. Please test this behavior.)_

**Example Input File (`upload_list.csv`):**

```csv
/home/user/data/sales.csv,monthly/reports/,year=2024 month=04
/opt/app/logs/service.log,application/logs/
/tmp/temp_config.yaml # Destination prefix defaults to CLI option
```

**Example Command:**

```bash
# Using the file with a base destination prefix
yawns s3 upload-list --destination-bucket my-upload-bucket --src upload_list.csv --destination-prefix uploads/batch1/ --max-concurrent 15

# Using stdin
cat upload_list.csv | yawns s3 upload-list --destination-bucket my-upload-bucket --src -
```

Similar to `copy-list`, the command will provide progress updates during the upload process, tracking uploaded and failed files.

## Error Handling

The tool utilizes `color-eyre` for enhanced error reporting. If you encounter an error, especially a crash, setting the `RUST_BACKTRACE=1` environment variable can provide detailed information helpful for debugging.

```bash
RUST_BACKTRACE=1 yawns s3 list-buckets # Example with backtrace
```

## Contributing

This project is open source _(assuming)_! Contributions are welcome. Whether it's adding new commands for other AWS services, improving existing functionality, enhancing documentation, or fixing bugs, feel free to open an issue or submit a pull request.

_(Link to contributing guidelines or just state general contribution principles)_

## License

This project is licensed under the `MIT License` _(assuming)_. See the `LICENSE` file for details.
