use anyhow::Result;
use clap::Parser;
use rocks_wal_tailer::{run_tailer_until_ctrlc, TailArgs};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = TailArgs::parse();
    run_tailer_until_ctrlc(args).await
}
