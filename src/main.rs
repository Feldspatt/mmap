mod memap;
mod avro;

use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufRead, BufWriter, Seek, Write};

use memmap2::{MmapMut, MmapOptions};
use crate::avro::{AvroType, Field, get_schema_from_json, write_data_to_file};

fn create_file(path: &str) {
    let mut writer = OpenOptions::new()
        .write(true)
        .create(true)
        .open(path).unwrap();

    let mut buf_writer = BufWriter::new(writer);

    let text = ["Hello, world!\n", "This is a test.\n", "Goodbye!"];

    for line in text.iter() {
        buf_writer.write(line.as_bytes()).unwrap();
    }
}

fn replace_char(path: &str, line: &str, char_index: u64) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();

    file.seek(io::SeekFrom::Start(char_index)).unwrap();
    file.write_all(line.as_bytes()).unwrap();
}

fn do_mmap_stuff(path: &str) {
    let file = OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();

    let mut mmap = unsafe { MmapOptions::new().populate().map_mut(&file).unwrap() };

    mmap.iter().for_each(|x| println!("{}", x));

    let insert = "shh";
    let insert_len = insert.len();

    let mut slice = mmap.split_at_mut(insert_len).0;

    slice.copy_from_slice(insert.as_ref());
    mmap.flush().unwrap();
}

fn capitalize_vowels(path: &str) {
    let file = OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();

    let mut mmap = unsafe { MmapOptions::new().populate().map_mut(&file).unwrap() };

    mmap.iter_mut().for_each(|x| {
        if x == &b'a' || x == &b'e' || x == &b'i' || x == &b'o' || x == &b'u' {
            *x = x.to_ascii_uppercase();
        }
    });

    mmap.flush().unwrap();
}



fn main() {
    let file = create_file("hello.txt");

    capitalize_vowels("hello.txt");

    let schema = get_schema_from_json("person.json");

    let data = vec![
        vec![
            Field {
                name: "name".to_string(),
                avro_type: AvroType::String{value: "John".to_string()},
            },
            Field {
                name: "age".to_string(),
                avro_type: AvroType::Int,
            },
        ],
        vec![
            Field {
                name: "name".to_string(),
                avro_type: AvroType::String{value: "Jane".to_string()},
            },
            Field {
                name: "age".to_string(),
                avro_type: AvroType::Int,
            },
        ],
    ];

    write_data_to_file("person.avro", &schema, data);

}
