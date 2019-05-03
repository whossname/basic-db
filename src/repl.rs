extern crate rustyline;

use repl::rustyline::error::ReadlineError;
use repl::rustyline::Editor;

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
                if user_input.len() > 0 {
                    evaluate_line(&user_input);
                    rl.add_history_entry(user_input);
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

fn evaluate_line(user_input: &String) {
    let first_char = user_input.chars().next().unwrap();
    if first_char == '.' { 
        do_meta(user_input);
    } else { 
        run_sql(user_input);
    };
}

fn do_meta(user_input: &String) {
    match user_input.as_ref() {
        "" => {
        },
        _ => {
            println!("Unknown meta command: {}\n", user_input.clone())
        },
    }
}

fn run_sql(user_input: &String) {
    match user_input.as_ref() {
        "" => {
        },
        _ => {
            println!("Invalid SQL: {}\n", user_input.clone())
        },
    }
}