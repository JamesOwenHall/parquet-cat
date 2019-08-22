extern crate clap;
extern crate parquet;
extern crate serde_json;

mod row_printer;

use clap::{App, Arg, ArgMatches};
use parquet::file::reader::{FileReader, SerializedFileReader};
use row_printer::RowPrinter;
use std::fs::File;
use std::path::Path;

fn main() {
    run_app(get_app().get_matches());
}

fn get_app<'a, 'b>() -> App<'a, 'b> {
    App::new("parquet-cat")
        .version("0.1.0")
        .about("Access data and metadata from parquet files")
        .arg(Arg::with_name("files")
            .required(true)
            .multiple(true))
}

fn run_app(matches: ArgMatches) {
    matches.values_of("files").unwrap().for_each(cat_file);
}

fn cat_file(path: &str) {
    let file = File::open(&Path::new(path)).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let schema = reader.metadata().file_metadata().schema().clone();

    let iter = reader.get_row_iter(None).unwrap();
    let mut printer = RowPrinter::new(schema);
    iter.for_each(|row| printer.println(&row));
}
