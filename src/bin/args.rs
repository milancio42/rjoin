use std::path::{Path, PathBuf,};
use std::error::Error;
use clap::{App, Arg, ArgGroup, };

pub fn app() -> App<'static, 'static> {
    App::new("rjoin")
        .author(crate_authors!())
        .version(crate_version!())
        .about("joins lines of two files with identical join fields.")
        .arg(Arg::with_name("show_left")
                 .short("l")
                 .long("show-left")
                 .help("print the unmatched lines from the left file"))
        .arg(Arg::with_name("show_right")
                 .short("r")
                 .long("show-right")
                 .help("print the unmatched lines from the right file"))
        .arg(Arg::with_name("show_both")
                 .short("b")
                 .long("show-both")
                 .help("print the matched lines"))
        .group(ArgGroup::with_name("show_any")
                        .args(&["show_left", "show_right", "show_both"])
                        .multiple(true))
        .arg(Arg::with_name("header")
                 .long("header")
                 .help("treat the first line in each file as field headers, print them without trying to pair them"))
        .arg(Arg::with_name("key")
                 .short("k")
                 .long("key")
                 .conflicts_with_all(&["left_key", "right_key"])
                 .takes_value(true)
                 .min_values(1)
                 .value_delimiter(",")
                 .value_name("FIELDS")
                 .help("equivalent to '--left-key=FIELDS --right-key=FIELDS'"))
        .arg(Arg::with_name("left_key")
                 .long("left-key")
                 .requires("right_key")
                 .takes_value(true)
                 .min_values(1)
                 .value_delimiter(",")
                 .value_name("FIELDS")
                 .help("join on these comma-separated fields in the left file")
                 .long_help(
"join on these comma-separated fields in the left file. The index 
starts with one and must not contain duplicates. The default is 1."))
        .arg(Arg::with_name("right_key")
                 .long("right-key")
                 .requires("left_key")
                 .takes_value(true)
                 .min_values(1)
                 .value_delimiter(",")
                 .value_name("FIELDS")
                 .help("join on these comma-separated fields in the right file")
                 .long_help(
"join on these comma-separated fields in the right file. The index 
starts with one and must not contain duplicates. The default is 1."))
        .arg(Arg::with_name("delimiter")
                 .long("delimiter")
                 .short("d")
                 .takes_value(true)
                 .value_name("CHAR")
                 .conflicts_with("in_delimiter")
                 .help("equivalent to '--in-delimiter=CHAR --out-delimiter=CHAR'"))
        .arg(Arg::with_name("in_delimiter")
                 .long("in-delimiter")
                 .takes_value(true)
                 .value_name("CHAR")
                 .requires("out_delimiter")
                 .conflicts_with_all(&["in_left_delimiter", "in_right_delimiter"])
                 .help("equivalent to '--in-left-delimiter=CHAR --in-right-delimiter=CHAR'"))
        .arg(Arg::with_name("out_delimiter")
                 .long("out-delimiter")
                 .takes_value(true)
                 .value_name("CHAR")
                 .requires("in_delimiter")
                 .help("use CHAR as output field delimiter")
                 .long_help(
"use CHAR as output field delimiter. It must be 1 byte long in utf-8."))
        .arg(Arg::with_name("in_left_delimiter")
                 .long("in-left-delimiter")
                 .takes_value(true)
                 .value_name("CHAR")
                 .requires_all(&["in_right_delimiter", "out_delimiter"])
                 .help("use CHAR as input field delimiter for the left file")
                 .long_help(
"use CHAR as input field delimiter for left file. It must be 1 byte long in utf-8."))
        .arg(Arg::with_name("in_right_delimiter")
                 .long("in-right-delimiter")
                 .takes_value(true)
                 .value_name("CHAR")
                 .requires("in_left_delimiter")
                 .help("use CHAR as input field delimiter for the right file")
                 .long_help(
"use CHAR as input field delimiter for the right file. It must be 1 byte long in utf-8."))
        .arg(Arg::with_name("terminator")
                 .long("terminator")
                 .short("t")
                 .takes_value(true)
                 .value_name("CHAR")
                 .conflicts_with("in_terminator")
                 .help("equivalent to '--in-terminator=CHAR --out-terminator=CHAR'"))
        .arg(Arg::with_name("in_terminator")
                 .long("in-terminator")
                 .takes_value(true)
                 .value_name("CHAR")
                 .requires("out_terminator")
                 .conflicts_with_all(&["in_left_terminator", "in_right_terminator"])
                 .help("equivalent to '--in-left-terminator=CHAR --in-right-terminator=CHAR'"))
        .arg(Arg::with_name("out_terminator")
                 .long("out-terminator")
                 .takes_value(true)
                 .value_name("CHAR")
                 .requires("in_terminator")
                 .help("use CHAR as output record terminator")
                 .long_help(
"use CHAR as output record terminator. It must be 1 byte long in utf-8."))
        .arg(Arg::with_name("in_left_terminator")
                 .long("in-left-terminator")
                 .takes_value(true)
                 .value_name("CHAR")
                 .requires_all(&["in_right_terminator", "out_terminator"])
                 .help("use CHAR as input record terminator for the left file")
                 .long_help(
"use CHAR as input record terminator for left file. It must be 1 byte long in utf-8."))
        .arg(Arg::with_name("in_right_terminator")
                 .long("in-right-terminator")
                 .takes_value(true)
                 .value_name("CHAR")
                 .requires("in_left_terminator")
                 .help("use CHAR as input record terminator for the right file")
                 .long_help(
"use CHAR as input record terminator for right file. It must be 1 byte long in utf-8."))
        .arg(Arg::with_name("LEFT_FILE")
                 .help("the left input file")
                 .required(true)
                 .index(1))
        .arg(Arg::with_name("RIGHT_FILE")
                 .help("the right input file")
                 .required(true)
                 .index(2))
}

pub struct Args {
    left_path: PathBuf,
    right_path: PathBuf,
    show_left: bool,
    show_right: bool,
    show_both: bool,
    left_key: Vec<usize>,
    right_key: Vec<usize>,
    in_left_delimiter: u8,
    in_right_delimiter: u8,
    out_delimiter: u8,
    in_left_terminator: u8,
    in_right_terminator: u8,
    out_terminator: u8,
    header: bool,
}

impl Args {
    pub fn parse() -> Result<Args, Box<Error>> {
        let matches = app().get_matches();

        let left_path = matches.value_of("LEFT_FILE").ok_or("expected LEFT_FILE")?;
        let right_path = matches.value_of("RIGHT_FILE").ok_or("expected RIGHT_FILE")?;

        let show_left = matches.is_present("show_left");
        let show_right = matches.is_present("show_right");
        let show_both = !matches.is_present("show_any") || matches.is_present("show_both");

        let header = matches.is_present("header");

        let key: Vec<usize> = match matches.values_of("key").map(|it| it.collect::<Vec<_>>()) {
            Some(v) => validate_key(v, "")?,
            None => vec![0],
        };
        let left_key: Vec<usize> = match matches.values_of("left_key")
                                                .map(|it| it.collect::<Vec<_>>()) {
            Some(v) => validate_key(v, "left ")?,
            None => key.clone(),
        };
        let right_key: Vec<usize> = match matches.values_of("right_key")
                                                 .map(|it| it.collect::<Vec<_>>()) {
            Some(v) => validate_key(v, "right ")?,
            None => key.clone(),
        };

        if left_key.len() != right_key.len() {
            return Err("the left key and the right key parameters have different lenght".into());
        }

        let delimiter = match matches.value_of("delimiter")
                                     .map(|s| s.as_bytes()) {
            Some(b) => {
                if b.len() > 1 {
                    return Err("the field delimiter must be 1 byte long in utf8".into());
                }
                b[0]
            }
            None => b','
        };
        let in_delimiter = match matches.value_of("in_delimiter")
                                        .map(|s| s.as_bytes()) {
            Some(b) => {
                if b.len() > 1 {
                    return Err("the input field delimiter must be 1 byte long in utf8".into());
                }
                b[0]
            }
            None => delimiter
        };
        let out_delimiter = match matches.value_of("out_delimiter")
                                         .map(|s| s.as_bytes()) {
            Some(b) => {
                if b.len() > 1 {
                    return Err("the output field delimiter must be 1 byte long in utf8".into());
                }
                b[0]
            }
            None => delimiter
        };
        let in_left_delimiter = match matches.value_of("in_left_delimiter")
                                             .map(|s| s.as_bytes()) {
            Some(b) => {
                if b.len() > 1 {
                    return Err("the left input field delimiter must be 1 byte long in utf8".into());
                }
                b[0]
            }
            None => in_delimiter
        };
        let in_right_delimiter = match matches.value_of("in_right_delimiter")
                                             .map(|s| s.as_bytes()) {
            Some(b) => {
                if b.len() > 1 {
                    return Err("the right input field delimiter must be 1 byte long in utf8".into());
                }
                b[0]
            }
            None => in_delimiter
        };

        let terminator = match matches.value_of("terminator")
                                   .map(|s| s.as_bytes()) {
            Some(b) => {
                if b.len() > 1 {
                    return Err("the record terminator must be 1 byte long in utf8".into());
                }
                b[0]
            }
            None => b'\n'
        };
        let in_terminator = match matches.value_of("in_terminator")
                                      .map(|s| s.as_bytes()) {
            Some(b) => {
                if b.len() > 1 {
                    return Err("the input record terminator must be 1 byte long in utf8".into());
                }
                b[0]
            }
            None => terminator
        };
        let out_terminator = match matches.value_of("out_terminator")
                                       .map(|s| s.as_bytes()) {
            Some(b) => {
                if b.len() > 1 {
                    return Err("the output record terminator must be 1 byte long in utf8".into());
                }
                b[0]
            }
            None => terminator
        };
        let in_left_terminator = match matches.value_of("in_left_terminator")
                                           .map(|s| s.as_bytes()) {
            Some(b) => {
                if b.len() > 1 {
                    return Err("the left input record terminator must be 1 byte long in \
                    utf8".into());
                }
                b[0]
            }
            None => in_terminator
        };
        let in_right_terminator = match matches.value_of("in_right_terminator")
                                            .map(|s| s.as_bytes()) {
            Some(b) => {
                if b.len() > 1 {
                    return Err("the right input record terminator must be 1 byte long in \
                    utf8".into());
                }
                b[0]
            }
            None => in_terminator
        };


        let args = Args { 
            left_path: left_path.into(),
            right_path: right_path.into(),
            show_left: show_left,
            show_right: show_right,
            show_both: show_both,
            left_key: left_key,
            right_key: right_key,
            in_left_delimiter: in_left_delimiter,
            in_right_delimiter: in_right_delimiter,
            out_delimiter: out_delimiter,
            in_left_terminator: in_left_terminator,
            in_right_terminator: in_right_terminator,
            out_terminator: out_terminator,
            header: header,
        };
        Ok(args)
    }
    pub fn left_path(&self) -> &Path {
        &self.left_path
    }
    pub fn right_path(&self) -> &Path {
        &self.right_path
    }
    pub fn show_left(&self) -> bool {
        self.show_left
    }
    pub fn show_right(&self) -> bool {
        self.show_right
    }
    pub fn show_both(&self) -> bool {
        self.show_both
    }
    pub fn left_key(&self) -> &[usize] {
        &self.left_key
    }
    pub fn right_key(&self) -> &[usize] {
        &self.right_key
    }
    pub fn in_left_delimiter(&self) -> u8 {
        self.in_left_delimiter
    }
    pub fn in_right_delimiter(&self) -> u8 {
        self.in_right_delimiter
    }
    pub fn out_delimiter(&self) -> u8 {
        self.out_delimiter
    }
    pub fn in_left_terminator(&self) -> u8 {
        self.in_left_terminator
    }
    pub fn in_right_terminator(&self) -> u8 {
        self.in_right_terminator
    }
    pub fn out_terminator(&self) -> u8 {
        self.out_terminator
    }
    pub fn header(&self) -> bool {
        self.header
    }
}

        
fn validate_key(k: Vec<&str>, which: &str) -> Result<Vec<usize>, Box<Error>> {
    let out = Ok(k)
        .map(|v| v.iter().map(|s| s.parse::<usize>())
                         .collect::<Vec<_>>())
        .and_then(|v| {
            let mut out: Vec<usize> = Vec::with_capacity(v.len());
            for (x, r) in v.iter().enumerate() {
                match *r {
                    Ok(i) => out.push(i),
                    Err(_) => return Err(format!("could not parse the {}key parameter at \
                                                  the position {}", which, x + 1).into()),
                }
            }
            Ok(out)
        })
        .and_then(|mut v| {
            if v.iter().any(|&i| i < 1) {
                return Err("the key fields must use 1-based numbering".into());
            }
            for i in v.iter_mut() {
               *i -= 1;
            }
            Ok(v)
        });
    out
}
