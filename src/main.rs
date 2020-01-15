extern crate rustyline;

use std::env;
#[macro_use]
mod serialise;
mod repl;
mod sql;
mod backend;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() == 1 {
        repl::run()
    } else {
        let query = &args[1];

        println!("{:?}", query);
    }
}
