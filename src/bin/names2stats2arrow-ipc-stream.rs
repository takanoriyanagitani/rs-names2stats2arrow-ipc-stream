use std::io::{self, Error};
use std::sync::Arc;

use arrow::ipc::writer::StreamWriter;
use clap::Parser;

use rs_names2stats2arrow_ipc_stream::{lines2batch_iter, schema, stdin2lines};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(long, default_value_t = 1024)]
    batch_size: usize,
}

fn main() -> Result<(), Error> {
    let cli = Cli::parse();

    let schema = schema();
    let mut writer = StreamWriter::try_new(io::stdout(), &schema).map_err(io::Error::other)?;

    let lines = stdin2lines();
    let batch_iter = lines2batch_iter(lines, Arc::clone(&schema), cli.batch_size)?;

    for rbat in batch_iter {
        let bat = rbat?;
        writer.write(&bat).map_err(io::Error::other)?;
    }

    writer.finish().map_err(io::Error::other)?;
    Ok(())
}
