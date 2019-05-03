use std::str::SplitWhitespace;

pub fn parse(user_input: &String) {
    let mut sql = user_input.split_whitespace();

    match sql.next() {
        Some("insert") => {
            parse_insert(&mut sql)
        },
        Some("select") => {
            parse_select(&mut sql)
        },
        _ => {
            println!("Unknown SQL statement `{}`\n", user_input.clone())
        },
    }
}

fn parse_insert(sql: &mut SplitWhitespace) {
    let id: i32 = sql.next().unwrap().parse().unwrap();
    let username = sql.next().unwrap();
    let email = sql.next().unwrap();

    if sql.next() == None {
    }
}

fn parse_select(sql: &mut SplitWhitespace) {
}

fn parse_int() {

}