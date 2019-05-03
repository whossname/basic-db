pub fn parse(user_input: &String) {
    let mut sql = user_input.split_whitespace();

    match sql.next() {
        Some("select") => {
            println!("{:?}\n", sql.next());
            println!("{:?}\n", sql.next());
        },
        Some("insert") => {
            println!("{:?}\n", sql.next());
            println!("{:?}\n", sql.next());
        },
        _ => {
            println!("Invalid SQL: {}\n", user_input.clone())
        },
    }
}