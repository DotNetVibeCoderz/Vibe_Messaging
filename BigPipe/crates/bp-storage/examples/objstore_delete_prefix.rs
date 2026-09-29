//! Deletes every object under a prefix (maintenance helper, e.g. cleaning up test runs).
//!
//!     cargo run -p bp-storage --example objstore_delete_prefix -- <store-url> <prefix> [--dry-run]
//!
//! Credentials come from the usual environment variables (AWS_*, AZURE_STORAGE_*, GOOGLE_*).
use bp_storage::ObjectStorage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: objstore_delete_prefix <store-url> <prefix> [--dry-run]");
        std::process::exit(2);
    }
    let dry = args.iter().any(|a| a == "--dry-run");
    let store = ObjectStorage::open(&args[1], 0)?;
    let objects = store.list(&args[2]).await?;
    for o in &objects {
        if !dry {
            store.delete(&o.key).await?;
        }
    }
    println!("{} {} object(s) under '{}'", if dry { "found" } else { "deleted" }, objects.len(), args[2]);
    Ok(())
}
