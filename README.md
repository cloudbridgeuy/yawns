# Yawns - An AWS CLI Helper

A Rust-based CLI tool to interact with AWS services using `clap`, `tokio`, and the AWS SDK for Rust.

## Installation

```bash
git clone <REPO_URL>
cargo install --path .
```

## Configuration

Set AWS credentials via environment variables:

```bash
export AWS_REGION=us-east-1
export AWS_PROFILE=default
```

## Usage

```bash
AWS CLI Helper - Shortcuts for common AWS operations

USAGE:
    yawns <SUBCOMMAND>

SUBCOMMANDS:
    kms     AWS KMS (AWS Key Management Service)
    s3      AWS S3 (Amazon Simple Storage Service)
```

### KMS Operations

List KMS keys with aliases:

```bash
yawns kms list-keys
```

Get key policy:

```bash
yawns kms get-policy --alias my-key-alias
```

### S3 Operations

List buckets:

```bash
yawns s3 list-buckets
```

Single object copy:

```bash
yawns s3 copy --source-bucket src-bkt --destination-bucket dst-bkt --src object.txt --dst new_object.txt
```

Batch copy with concurrency control:

```bash
yawns s3 copy-list \
    --source-bucket src-bkt \
    --destination-bucket dst-bkt \
    --max-concurrent 20 \
    --metadata "key1=value1" \
    --metadata "key2=value2" \
    < objects.list
```

## Features

- Verbose output with `--verbose` flag
- Environment variable support for all credentials
- Concurrent batch operations with configurable parallelism
- Rich error reporting with `color_eyre`
- Tabular output for list operations

## Error Handling

The tool uses `color_eyre` for error reporting. Enable backtraces:

```bash
RUST_BACKTRACE=1 yawns [COMMAND]
```

## Logging

Set log level using `RUST_LOG`:

```bash
RUST_LOG=info yawns [COMMAND]
```

## Contributing

1. Fork the repository
2. Create feature branch
3. Submit PR with tests and documentation

## License

MIT License

## Acknowledgements

- `clap` for CLI parsing
- `tokio` for async runtime
- AWS Rust SDK for cloud integrations
- `color_eyre` for error reporting
