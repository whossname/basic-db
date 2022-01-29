extern crate serde;
extern crate sysconf;

use super::super::record;
use super::Column;
use super::ColumnType;
use super::Database;

use std::collections::HashMap;
use std::error;

impl Database {
    pub fn insert_records(
        &mut self,
        table_name: String,
        row_hashs: Vec<HashMap<String, String>>,
    ) -> Result<(), Box<dyn error::Error>> {
        let (page_number, columns) = self.describe_table(table_name)?;

        row_hashs
            .into_iter()
            .map(|row_hash| {
                let row = create_row(&columns, row_hash);
                let record = record::create_record(row);
                record::insert_record(self, record, page_number);
            })
            .count();

        self.commit()?;
        Ok(())
    }

    pub fn insert_record(
        &mut self,
        table_name: String,
        row_hash: HashMap<String, String>,
    ) -> Result<(), Box<dyn error::Error>> {
        let (page_number, columns) = self.describe_table(table_name)?;
        let row = create_row(&columns, row_hash);
        let record = record::create_record(row);
        record::insert_record(self, record, page_number);
        self.commit()?;
        Ok(())
    }

    pub fn select_all_records(
        &mut self,
        table_name: String,
    ) -> Result<Vec<Vec<Column>>, Box<dyn error::Error>> {
        let record_filter = |_row: &Vec<Column>| true;
        let column_filter = |row: Vec<Column>| row;
        self.select_records(table_name, record_filter, column_filter)
    }

    pub fn select_records<RecF, ColF>(
        &mut self,
        table_name: String,
        record_filter: RecF,
        column_filter: ColF,
    ) -> Result<Vec<Vec<Column>>, Box<dyn error::Error>>
    where
        RecF: Fn(&Vec<Column>) -> bool,
        ColF: FnMut(Vec<Column>) -> Vec<Column>,
    {
        let (page_number, _columns) = self.describe_table(table_name)?;
        record::select_records(self, page_number, record_filter, column_filter)
    }
}

fn create_row(
    columns: &Vec<(String, ColumnType)>,
    row_hash: HashMap<String, String>,
) -> Vec<Column> {
    let row = columns
        .into_iter()
        .map(|col| match col {
            (col_name, ColumnType::Integer) => {
                let column_wrapper = |value| Column::Integer(value);
                parse_column(&row_hash, col_name, column_wrapper)
            }
            (col_name, ColumnType::Real) => {
                let column_wrapper = |value| Column::Real(value);
                parse_column(&row_hash, col_name, column_wrapper)
            }
            (col_name, ColumnType::Text) => {
                let column_wrapper = |value| Column::Text(value);
                parse_column(&row_hash, col_name, column_wrapper)
            }
            (col_name, ColumnType::Blob) => {
                let value = row_hash.get(col_name);
                match value {
                    Some(value) => {
                        // TODO parse binary correctly
                        let value = value.clone().into_bytes();
                        Column::Blob(value)
                    }
                    None => Column::Null(),
                }
            }
        })
        .collect();
    row
}

fn parse_column<T: std::str::FromStr, ColFn>(
    row_hash: &HashMap<String, String>,
    col_name: &String,
    column_wrapper: ColFn,
) -> Column
where
    ColFn: Fn(T) -> Column,
{
    let value = row_hash.get(col_name);
    match value {
        Some(value) => {
            let value = value.parse();
            match value {
                Ok(value) => column_wrapper(value),
                _ => panic!("input value not compatible with data type"),
            }
        }
        None => Column::Null(),
    }
}
