# YAWNS

## Parallelization

```rust
use aws_sdk_kms::Client;
use futures::future::join_all;
use prettytable::Table;
use std::error::Error;

pub async fn list_keys(client: Client) -> Result<(), Box<dyn Error>> {
    let resp = client.list_keys().send().await?;

    log::info!("Getting the list of KMS keys");
    let keys = resp.keys.unwrap_or_default();

    let mut table = Table::new();
    table.set_titles(prettytable::row!["Arn", "Id"]);

    let alias_futures = keys.into_iter().map(|key| {
        let client = client.clone();
        async move {
            let key_id = key.key_id.unwrap_or_default();
            log::info!("Getting aliases of KMS key {}", key_id);
            let resp = client.list_aliases().key_id(key_id).send().await?;
            let aliases = resp.aliases.unwrap_or_default();
            let alias_names = aliases
                .iter()
                .map(|alias| alias.alias_name.as_deref().unwrap_or_default())
                .collect::<Vec<&str>>()
                .join(", ");
            Ok((key.key_arn.unwrap_or_default(), alias_names)) as Result<_, Box<dyn Error>>
        }
    });

    let results = join_all(alias_futures).await;

    for result in results {
        match result {
            Ok((arn, alias_names)) => {
                table.add_row(prettytable::row![arn, alias_names]);
            }
            Err(e) => {
                log::error!("Error getting aliases: {}", e);
            }
        }
    }

    aprintln!("{}", table.to_string());

    Ok(())
}
```
