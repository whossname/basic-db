#![allow(dead_code)]
extern crate rustyline;

use std::env;
#[macro_use]
mod serialise;
mod backend;
mod repl;
mod sql;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() == 1 {
        repl::run()
    } else {
        let query = &args[1];

        println!("{:?}", query);
    }
}
