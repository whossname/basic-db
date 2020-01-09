use std::error;
use std::fmt;
use std::num;
use std::str::SplitWhitespace;

#[derive(Debug)]
pub struct SqlError;

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "invalid first item to double")
    }
}

impl error::Error for SqlError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}

impl From<num::ParseIntError> for SqlError {
    fn from(_err: num::ParseIntError) -> SqlError {
        SqlError
    }
}

trait TypeCheck {
    fn check_type(self, col_name: String) -> Self;
}

impl TypeCheck for Result<i32, std::num::ParseIntError> {
    fn check_type(self, col_name: String) -> Self {
        match &self {
            Err(msg) => {
                println!(
                    "Invalid data type for column {}, expected integer",
                    col_name
                );
                println!("{}", msg);
            }
            Ok(_) => {}
        }
        self
    }
}

pub fn parse(user_input: &String) -> Result<(), SqlError> {
    let mut sql = user_input.split_whitespace();

    match sql.next() {
        Some("insert") => parse_insert(&mut sql),
        Some("select") => parse_select(&sql),
        _ => {
            println!("Unknown SQL statement `{}`\n", user_input.clone());
            Err(SqlError)
        }
    }
}

fn parse_insert(sql: &mut SplitWhitespace) -> Result<(), SqlError> {
    let id = sql
        .next()
        .expect("Missing Value")
        .parse::<i32>()
        .check_type("id".to_string())?;

    let username = sql.next().expect("Missing Value");
    let email = sql.next().expect("Missing Value");

    if sql.next() == None {
        let record = (id, username, email);
        println!("new record {:?}", record);
    }
    Ok(())
}

fn parse_select(_sql: &SplitWhitespace) -> Result<(), SqlError> {
    Ok(())
}
