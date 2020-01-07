use rustyline::error::ReadlineError;
use rustyline::Editor;

use sql;

pub fn run() {
    // `()` can be used when no completer is required
    let mut rl = Editor::<()>::new();
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(user_input) => {
                let first_char = user_input.chars().next();
                match first_char {
                    None => {}

                    Some('.') => {
                        do_meta(&user_input);
                        rl.add_history_entry(user_input);
                    }

                    Some(_) => {
                        sql::parse(&user_input);
                        rl.add_history_entry(user_input);
                    }
                }
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

fn do_meta(user_input: &String) {
    match user_input.as_ref() {
        "" => {}
        _ => println!("Unknown meta command: {}\n", user_input.clone()),
    }
}
