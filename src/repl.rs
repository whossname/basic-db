extern crate rustyline;

use rustyline::error::ReadlineError;
use rustyline::Editor;

pub fn run() {
    // `()` can be used when no completer is required
    let mut rl = Editor::<()>::new();
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(cmd) => {
                let first_char = cmd.chars().next().unwrap();
                if first_char == "." { 
                    do_meta(cmd.clone());
                } else { 
                    run_cmd(cmd.clone());
                };
                rl.add_history_entry(cmd);
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history("history.txt").unwrap();
}

fn do_meta(cmd: String) {
    match cmd.as_ref() {
        "" => {
        },
        _ => {
            println!("Unknown command: {}\n", cmd.clone())
        },
    }
}

fn run_cmd(cmd: String) {
    match cmd.as_ref() {
        "" => {
        },
        _ => {
            println!("Unknown command: {}\n", cmd.clone())
        },
    }
}