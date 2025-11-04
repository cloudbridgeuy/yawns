# CLAUDE.md - Yawns Developer Guide

## Project Overview

Yawns is a Rust-based AWS CLI helper tool designed to streamline common AWS operations. It provides focused, efficient commands for AWS S3 and KMS services, with emphasis on bulk operations and concurrent processing.

**Key Characteristics:**
- Built with Rust for performance and safety
- Async-first architecture using Tokio
- Concurrent operations with configurable limits
- Workspace-based project structure
- Comprehensive error handling with color-eyre

---

## Workspace Structure

The project uses Cargo workspaces to organize code:

```
yawns/
├── crates/
│   └── yawns/          # Main application crate
│       ├── src/
│       │   ├── main.rs      # CLI entry point and routing
│       │   ├── kms.rs       # KMS service commands
│       │   ├── s3.rs        # S3 service commands
│       │   ├── aws.rs       # AWS SDK configuration
│       │   ├── error.rs     # Custom error types
│       │   └── prelude.rs   # Common imports and utilities
│       └── Cargo.toml
├── xtask/              # Build automation and tasks
│   ├── src/
│   └── Cargo.toml
├── Cargo.toml          # Workspace root
└── README.md
```

### Workspace Configuration

The root `Cargo.toml` defines shared package metadata:
- **Edition**: 2021
- **License**: MIT
- **Resolver**: Version 2

---

## Main Components

### 1. CLI Structure (`main.rs`)

The application uses **clap** for declarative CLI parsing with derive macros:

- **App**: Root command structure with global options
- **Global**: Shared flags (region, profile, verbose)
- **SubCommands**: Top-level command categories (KMS, S3)

**Entry Point Flow:**
```
Parse CLI args → Match subcommand → Delegate to service module → Run async operation
```

### 2. KMS Module (`kms.rs`)

Provides AWS Key Management Service operations:

**Commands:**
- `list-keys`: List all KMS keys with their aliases
- `get-policy`: Retrieve key policy by alias

**Key Patterns:**
- Concurrent alias fetching using `futures::join_all`
- Pretty table output formatting
- Error handling with Result propagation

### 3. S3 Module (`s3.rs`)

Provides Amazon S3 operations with focus on bulk processing:

**Commands:**
- `list-buckets`: List all S3 buckets
- `copy`: Copy single object between buckets
- `copy-list`: Bulk copy operations from CSV input
- `count-files`: Count objects with optional prefix filtering
- `upload-list`: Bulk upload local files to S3

**Key Patterns:**
- Semaphore-based concurrency control
- Progress tracking with atomic counters
- CSV parsing for bulk operations
- Metadata support for objects
- Stdin/file input support via `clap-stdin`

### 4. AWS Configuration (`aws.rs`)

Handles AWS SDK configuration:

```rust
pub async fn get_sdk_config_from_global(global: crate::Global) -> Result<aws_config::SdkConfig>
```

**Features:**
- Region override support
- Profile selection
- Environment-based configuration fallback

### 5. Error Handling (`error.rs`)

Custom error types using `thiserror` for domain-specific errors.

### 6. Prelude (`prelude.rs`)

Common imports and utilities used across modules:
- Error types and Result
- color-eyre utilities (eyre!, Context, OptionExt)
- anstream printing (aprintln, aeprintln)
- Table formatting helper (`new_table()`)

---

## Core Dependencies

### CLI & Parsing
- **clap** (4.5.37): Command-line argument parsing with derive macros, env var support
- **clap-stdin** (0.6.0): File or stdin input handling

### Async Runtime
- **tokio** (1.44.2): Async runtime with full feature set
- **futures** (0.3.31): Async utilities and combinators

### AWS SDK
- **aws-config** (1.6.2): AWS SDK configuration
- **aws-sdk-kms** (1.66.0): KMS service client
- **aws-sdk-s3** (1.83.0): S3 service client
- **aws-types** (1.3.7): Common AWS types
- **aws-smithy-types** (1.3.1): Smithy type system

### Error Handling & Logging
- **color-eyre** (0.6.3): Enhanced error reporting with color
- **thiserror** (2.0.12): Custom error type derivation
- **env_logger** (0.11.8): Environment-based logger
- **log** (0.4.27): Logging facade

### Serialization & Formatting
- **serde** (1.0.219): Serialization framework
- **prettytable** (0.10.0): ASCII table formatting
- **anstream** (0.6.18): ANSI stream handling

### Build Tools (xtask)
- **clap** (4.1.8): CLI for build tasks
- **duct** (0.13.6): Process execution
- **chrono** (0.4.24): Date/time operations
- **bunt** (0.2.8): Colored terminal output
- **color-eyre** (0.6.2): Error handling

---

## Code Patterns & Conventions

### 1. Async/Await Architecture

All I/O operations are asynchronous:

```rust
#[tokio::main]
async fn main() -> Result<()> {
    // Async operations
}
```

### 2. Error Propagation

Uses `color-eyre::Result` throughout with `?` operator for clean error handling:

```rust
pub async fn run(app: App, global: crate::Global) -> Result<()> {
    let client = create_client(&global).await?;
    execute_command(client).await?;
    Ok(())
}
```

### 3. Concurrent Operations

Bulk operations use semaphores for concurrency control:

```rust
let semaphore = Arc::new(Semaphore::new(max_concurrent));
let permit = semaphore.acquire().await?;
// Perform operation
drop(permit);
```

### 4. Progress Tracking

Long-running operations use atomic counters and periodic updates:

```rust
let completed = Arc::new(AtomicUsize::new(0));
let failed = Arc::new(AtomicUsize::new(0));
// Spawn progress reporter task
```

### 5. Builder Pattern

AWS SDK uses builder pattern extensively:

```rust
client
    .copy_object()
    .source(source_key)
    .destination(dest_key)
    .send()
    .await?
```

### 6. Derive Macros

Heavy use of derive macros for CLI and serialization:

```rust
#[derive(Debug, clap::Parser)]
#[derive(Debug, clap::Args, serde::Serialize, serde::Deserialize)]
```

---

## Functional Core - Imperative Shell

We advocate the use of this pattern when writing code for this repo.

The pattern is based on separating code into two distinct layers:

**Functional Core**: Pure, testable business logic free of side effects (no I/O, no external state mutations). It operates only on the data it's given.

**Imperative Shell**: Responsible for side effects like database calls, network requests, and sending emails. It uses the functional core to perform business logic.

### Example Transformation

**Before (mixed logic and side effects):**

```javascript
function sendUserExpiryEmail(): void {
  for (const user of db.getUsers()) {
    if (user.subscriptionEndDate > Date.now()) continue;
    if (user.isFreeTrial) continue;
    email.send(user.email, "Your account has expired " + user.name + ".");
  }
}
```

**After (separated):**

**Functional Core:**
```javascript
getExpiredUsers(users, cutoff) // pure filtering logic
generateExpiryEmails(users)    // pure email generation
```

**Imperative Shell:**
```javascript
email.bulkSend(generateExpiryEmails(getExpiredUsers(db.getUsers(), Date.now())))
```

### Benefits

- **More testable**: Core logic can be tested in isolation without mocking I/O
- **More maintainable**: Pure functions are easier to reason about and modify
- **More reusable**: Business logic (e.g., `getExpiredUsers`) can be reused for other features like reminder emails
- **More adaptable**: Imperative shell can be swapped out (e.g., change from email to SMS) without touching core logic

### Applying to Yawns

When adding new features to Yawns:

1. **Separate concerns**: Extract pure logic (filtering, transformation, validation) from I/O operations (AWS API calls, file reading)

2. **Example - S3 filtering:**
   ```rust
   // Functional Core - pure filtering logic
   fn filter_objects_by_prefix(objects: &[Object], prefix: &str) -> Vec<&Object> {
       objects.iter()
           .filter(|obj| obj.key().unwrap_or("").starts_with(prefix))
           .collect()
   }

   // Imperative Shell - I/O and coordination
   pub async fn list_filtered_objects(client: &S3Client, bucket: &str, prefix: &str) -> Result<()> {
       let response = client.list_objects_v2().bucket(bucket).send().await?;
       let objects = response.contents().unwrap_or_default();
       let filtered = filter_objects_by_prefix(objects, prefix);
       print_results(filtered);
       Ok(())
   }
   ```

3. **Test the core**: Write unit tests for pure functions without needing AWS credentials or mocks

4. **Keep shells thin**: Imperative shell should be primarily about orchestration and I/O, delegating logic to the core

The pattern is based on [Gary Bernhardt's original talk](https://www.destroyallsoftware.com/screencasts/catalog/functional-core-imperative-shell) on the concept.

---

## Development Workflow

### Building

```bash
# Build in development mode
cargo build

# Build with optimizations
cargo build --release

# Install locally
cargo install --path .
```

### Running

```bash
# Run directly with cargo
cargo run -- s3 list-buckets

# After installation
yawns s3 list-buckets --region us-west-2
```

### Testing

```bash
# Run all tests
cargo test

# Run with logging
RUST_LOG=debug cargo test

# Run specific test
cargo test test_name
```

### Linting & Formatting

```bash
# Format code
cargo fmt

# Run clippy
cargo clippy -- -D warnings
```

---

## Environment Variables

### AWS Configuration
- `AWS_REGION`: Default region (default: us-east-1)
- `AWS_PROFILE`: AWS profile name (default: default)
- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key

### Application Settings
- `YAWNS_VERBOSE`: Enable verbose output (default: false)
- `YAWNS_KMS_ALIAS`: Default KMS key alias

### S3 Command Defaults
- `AWS_S3_SRC_BUCKET`: Source bucket for copy operations
- `AWS_S3_DST_BUCKET`: Destination bucket
- `AWS_S3_SRC_OBJECT`: Source object key
- `AWS_S3_DST_OBJECT`: Destination object key
- `AWS_S3_SRC_OBJECT_LIST`: List file path
- `AWS_S3_SRC_OBJECT_PREFIX`: Source prefix
- `AWS_S3_DST_OBJECT_PREFIX`: Destination prefix
- `AWS_S3_MAX_CONCURRENT`: Max concurrent operations (default: 10)
- `AWS_S3_BUCKET`: Bucket name for count operations
- `AWS_S3_OBJECT_PREFIX`: Object prefix filter

### Debugging
- `RUST_LOG`: Log level (trace, debug, info, warn, error)
- `RUST_BACKTRACE`: Enable backtraces (0, 1, full)

---

## Performance Considerations

### Concurrency Tuning

The `--max-concurrent` flag controls parallelism for bulk operations:

- **Higher values**: Faster throughput but more memory/network usage
- **Lower values**: Better for rate-limited APIs or constrained resources
- **Default (10)**: Balanced for most use cases

### Progress Reporting

Progress updates occur every 5 seconds by default to balance informativeness with overhead.

### Async Runtime

Tokio's "full" feature set is enabled, providing:
- Multi-threaded runtime
- I/O drivers
- Time utilities
- Sync primitives

---

## Adding New Features

### Adding a New S3 Command

1. Add command variant to `s3::Commands` enum
2. Define options struct with `#[derive(Debug, clap::Args)]`
3. Implement handler function following async patterns
4. Add match arm in `s3::run()`
5. Update README.md with command documentation

### Adding a New AWS Service

1. Add dependency in `Cargo.toml` (e.g., `aws-sdk-dynamodb`)
2. Create new module file (e.g., `dynamodb.rs`)
3. Define CLI structure with commands
4. Implement async handler functions
5. Add service variant to root `SubCommands` enum
6. Add match arm in `main()`

---

## Troubleshooting

### AWS Credentials Issues

```bash
# Verify credentials
aws sts get-caller-identity --profile <profile>

# Check yawns configuration
yawns --profile <profile> --region <region> --verbose s3 list-buckets
```

### Performance Issues

- Reduce `--max-concurrent` for rate limiting
- Check network bandwidth
- Verify AWS service limits

### Error Debugging

```bash
# Enable verbose logging
RUST_LOG=debug yawns s3 copy-list ...

# Enable backtraces
RUST_BACKTRACE=full yawns s3 copy-list ...
```

---

## Future Enhancements

Potential areas for expansion:

- Additional AWS services (DynamoDB, Lambda, EC2)
- Retry logic with exponential backoff
- Configuration file support
- Shell completion scripts
- Progress bars with indicatif
- JSON output mode
- Dry-run mode for destructive operations
- Unit and integration test coverage
- Benchmarking suite

---

## References

- [Rust Book](https://doc.rust-lang.org/book/)
- [Tokio Documentation](https://tokio.rs/)
- [AWS SDK for Rust](https://aws.amazon.com/sdk-for-rust/)
- [Clap Documentation](https://docs.rs/clap/)
- [Functional Core, Imperative Shell](https://www.destroyallsoftware.com/screencasts/catalog/functional-core-imperative-shell)
