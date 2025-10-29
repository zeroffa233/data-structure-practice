#![allow(unused)]
use fs::*;
use io::*;
use rand::Rng;
use std::time::{Duration, Instant};
use std::*;

pub struct SourceFileGenerator {
    pub n: u64,
    pub min: i32,
    pub max: i32,
    pub output_file_path: String,
}

impl SourceFileGenerator {
    pub fn new(n: u64, min: i32, max: i32, output_file_path: String) -> Self {
        let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
        let output_file_path =
            format!("{}/data/project_3/{}", cargo_manifest_dir, output_file_path);
        Self {
            n,
            min,
            max,
            output_file_path,
        }
    }
    pub fn generate_file(&self) {
        println!("> 开始生成随机数字序列文件...");
        let file = File::create(&self.output_file_path).expect("Unable to create file");
        let mut writer = BufWriter::with_capacity(1024, file);
        for _ in 0..self.n {
            let num = rand::rng().random_range(self.min..=self.max);
            write!(writer, "{} ", num).expect("Unable to write data");
        }
        println!(
            "> 随机数字序列文件生成完毕，文件路径：{}",
            self.output_file_path
        );
    }
}
pub fn run() {
    let mut generator = SourceFileGenerator::new(100, 0, 100, "nums.txt".into());
    generator.generate_file();
}

pub struct RunGenerator {
    pub input_file_path: String,
    pub output_file_path: String,
    pub buffer1: 
}

impl RunGenerator {
    pub fn new(input_file_path: String, output_file_path: String) -> Self {
        let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
        let input_file_path = format!("{}/data/project_3/{}", cargo_manifest_dir, input_file_path);
        let output_file_path =
            format!("{}/data/project_3/{}", cargo_manifest_dir, output_file_path);
        Self {
            input_file_path,
            output_file_path,
        }
    }
}
