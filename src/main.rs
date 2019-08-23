extern crate chrono;
extern crate clap;
extern crate parquet;
extern crate serde_json;
extern crate signal_hook;

mod file_metadata;
mod row_printer;

use clap::{App, Arg, ArgMatches};
use file_metadata::FileMetadata;
use parquet::file::reader::{FileReader, SerializedFileReader};
use row_printer::RowPrinter;
use std::fs::File;
use std::path::Path;

fn main() {
    handle_broken_pipe();
    run_app(get_app().get_matches());
}

fn get_app<'a, 'b>() -> App<'a, 'b> {
    App::new("parquet-cat")
        .version("0.1.0")
        .about("Access data and metadata from parquet files")
        .arg(Arg::with_name("files")
            .required(true)
            .multiple(true))
        .arg(Arg::with_name("metadata")
            .short("m")
            .long("metadata")
            .help("Prints metadata about each file instead of its contents"))
}

fn run_app(matches: ArgMatches) {
    if matches.is_present("metadata") {
        matches.values_of("files").unwrap().for_each(print_file_metadata);
    } else {
        matches.values_of("files").unwrap().for_each(cat_file);
    }
}

fn print_file_metadata(path: &str) {
    let file = File::open(&Path::new(path)).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = FileMetadata::from_parquet(path.to_owned(), reader.metadata().file_metadata());
    let serialized = serde_json::to_string_pretty(&metadata).unwrap();
    println!("{}", &serialized);
}

fn cat_file(path: &str) {
    let file = File::open(&Path::new(path)).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let schema = reader.metadata().file_metadata().schema().clone();

    let iter = reader.get_row_iter(None).unwrap();
    let mut printer = RowPrinter::new(schema);
    iter.for_each(|row| printer.println(&row));
}

// By default, Rust apps will panic if they can't write to stdout/stderr. This
// function handles SIGPIPE which is sent when its output is closed. This app
// is useless if it can't print, so it simply exits the app.
fn handle_broken_pipe() {
    unsafe {
        let _ = signal_hook::register(signal_hook::SIGPIPE, || {
            std::process::exit(1);
        });
    }
}
