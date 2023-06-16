use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Editor, Result};

use rustyline::history::FileHistory;
use sql;

enum ReplState {
    Continue,
    Exit,
}

pub fn run() {
    // `()` can be used when no completer is required
    let mut rl = DefaultEditor::new().unwrap();
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(val) => match user_input(val, &mut rl) {
                ReplState::Exit => {
                    return;
                }
                ReplState::Continue => {}
            },
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
    #[cfg(feature = "with-file-history")]
    rl.save_history("history.txt");
}

fn user_input(line: String, rl: &mut Editor<(), FileHistory>) -> ReplState {
    let first_char = line.chars().next();
    match first_char {
        None => ReplState::Continue,

        Some('.') => {
            rl.add_history_entry(&line);
            do_meta(&line)
        }

        Some(_) => {
            rl.add_history_entry(&line);
            sql::parse(&line);
            ReplState::Continue
        }
    }
}

fn do_meta(user_input: &String) -> ReplState {
    match user_input.as_ref() {
        ".exit" => ReplState::Exit,
        _ => {
            println!("Unknown meta command: {}\n", user_input.clone());
            ReplState::Continue
        }
    }
}
