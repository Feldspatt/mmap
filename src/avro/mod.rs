use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use apache_avro::{Schema, Writer};
use apache_avro::types::{Record, Value};

pub enum AvroType {
    String{ value: String},
    Long{value: i64},
}

impl From<AvroType> for Value {
    fn from(avro_type: AvroType) -> Self {
        match avro_type {
            AvroType::String{value} => Value::String(value),
            AvroType::Long{value} => Value::Long(value),
        }
    }
}


pub struct Field {
    pub name: String,
    pub avro_type: AvroType,
}

pub fn get_schema_from_json(path: &str) -> Schema {
    let mut file = File::open(path).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let schema = Schema::parse_str(&contents).unwrap();
    return schema;
}

pub fn write_data_to_file(path: &str, schema: &Schema, data: Vec<Vec<Field>>) {
    let mut writer = Writer::new(&schema, Vec::new());

    for datum in data {
        let mut record = Record::new(writer.schema()).unwrap();

        for field in &datum {
            record.put(&field.name, field.avro_type.value);
        }
        writer.append(record).unwrap();    }

    writer.into_inner().unwrap();

    let mut file = OpenOptions::new().write(true).create(true).open(path).unwrap();

    file.write_all(writer.into_inner().unwrap().as_ref()).unwrap();
}