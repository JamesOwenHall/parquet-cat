use std::collections::HashMap;
use std::error::Error;
use std::fmt;

use chrono::{DateTime, NaiveDateTime, SecondsFormat, Utc};
use parquet::basic::Type as BasicType;
use parquet::record::{Row, RowAccessor};
use parquet::schema::types::{Type, TypePtr};
use serde_json::{Number, Value as JsonValue};

pub struct RowPrinter {
    fields: Vec<TypePtr>,
    map: HashMap<String, JsonValue>,
}

impl RowPrinter {
    pub fn new(schema: Type) -> Self {
        RowPrinter{
            fields: schema.get_fields().to_vec(),
            map: HashMap::new(),
        }
    }

    pub fn println(&mut self, row: &Row) -> Result<(), UnsupportedType> {
        self.map.clear();
        for (i, field) in self.fields.iter().enumerate() {
            let (key, value) = Self::as_json_field(field, row, i)?;
            self.map.insert(key, value);
        }

        let serialized = serde_json::to_string(&self.map).unwrap();
        println!("{}", serialized);
        Ok(())
    }

    fn as_json_field(field: &TypePtr, row: &Row, i: usize) -> Result<(String, JsonValue), UnsupportedType> {
        match field.as_ref() {
            Type::PrimitiveType{
                basic_info,
                physical_type,
                type_length: _,
                scale: _,
                precision: _,
            } => {
                let value = match physical_type {
                    BasicType::BOOLEAN => JsonValue::Bool(row.get_bool(i).unwrap()),
                    BasicType::INT32 => JsonValue::Number(Number::from(row.get_int(i).unwrap())),
                    BasicType::INT64 => JsonValue::Number(Number::from(row.get_long(i).unwrap())),
                    BasicType::INT96 => {
                        let nanos = row.get_timestamp(i).unwrap();
                        let ndt = NaiveDateTime::from_timestamp((nanos / 1000) as i64, (nanos % 1000) as u32);
                        JsonValue::String(DateTime::<Utc>::from_utc(ndt, Utc).to_rfc3339_opts(SecondsFormat::AutoSi, true))
                    },
                    BasicType::DOUBLE => JsonValue::Number(Number::from_f64(row.get_double(i).unwrap()).unwrap()),
                    BasicType::BYTE_ARRAY => JsonValue::String(row.get_string(i).unwrap().clone()),
                    _ => return Err(UnsupportedType(format!("{}", physical_type))),
                };
                Ok((basic_info.name().to_owned(), value))
            },
            Type::GroupType{basic_info: _,fields: _} => Err(UnsupportedType(format!("group"))),
        }
    }
}

pub struct UnsupportedType(String);

impl fmt::Display for UnsupportedType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} fields are not supported", self.0)
    }
}

impl fmt::Debug for UnsupportedType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} fields are not supported", self.0)
    }
}

impl Error for UnsupportedType {}
