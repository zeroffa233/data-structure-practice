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
            format!("{}/data/project_2/{}", cargo_manifest_dir, output_file_path);
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

pub struct RunGenerator {
    pub run_length: u32,
    pub input_file_path: String,
    pub output_file_path: String,
}

impl RunGenerator {
    pub fn new(run_length: u32, input_file_path: String, output_file_path: String) -> Self {
        let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
        let input_file_path = format!("{}/data/project_2/{}", cargo_manifest_dir, input_file_path);
        let output_file_path =
            format!("{}/data/project_2/{}", cargo_manifest_dir, output_file_path);
        Self {
            run_length,
            input_file_path,
            output_file_path,
        }
    }

    pub fn generate_run_file(&self) {
        println!("> 开始生成顺串文件...");
        let input_file = File::open(&self.input_file_path).expect("Unable to open input file");

        let reader = BufReader::new(input_file);

        let mut run_count = 0;

        let mut numbers: Vec<i32> = Vec::with_capacity(self.run_length as usize);
        let mut current_number = String::new();

        // 4. 逐字节迭代处理
        for byte_result in reader.bytes() {
            let byte = byte_result.expect("Unable to read byte");
            let ch = byte as char;

            if ch.is_digit(10) || (ch == '-' && current_number.is_empty()) {
                current_number.push(ch);
            } else if ch.is_whitespace() && !current_number.is_empty() {
                // b. 遇到空格或换行，解析当前数字
                if let Ok(num) = current_number.parse::<i32>() {
                    numbers.push(num);
                }
                current_number.clear();

                if numbers.len() >= self.run_length as usize {
                    numbers.sort_unstable();
                    let output_file_path =
                        format!("{}/run_{}.txt", self.output_file_path, run_count);
                    let output_file = File::create(&output_file_path)
                        .expect("Unable to open or create output file");
                    let mut writer = BufWriter::new(output_file);
                    for num in &numbers {
                        writer
                            .write_all(format!("{} ", num).as_bytes())
                            .expect("Unable to write data");
                    }
                    run_count += 1;
                    numbers.clear();
                }
            }
        }
        println!("> 顺串文件生成完毕，文件路径：{}", self.output_file_path);
    }
}

pub struct Merger {
    pub input_file_path: String,
    pub output_file_path: String,
    pub merge_pass_count: u32,
}

impl Merger {
    pub fn new(input_file_path: String, output_file_path: String) -> Self {
        let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
        let input_file_path = format!("{}/data/project_2/{}", cargo_manifest_dir, input_file_path);
        let output_file_path =
            format!("{}/data/project_2/{}", cargo_manifest_dir, output_file_path);
        let merge_pass_count = 1;
        Self {
            input_file_path,
            output_file_path,
            merge_pass_count,
        }
    }

    pub fn merge(&mut self) {
        println!("> 开始进行归并排序...");
        loop {
            let dir = fs::read_dir(self.input_file_path.clone()).expect(
                format!("Unable to read input directory: {}", self.input_file_path).as_str(),
            );
            let runs_count = dir.count() as u32;
            let merged_runs_count = (runs_count + 1) / 2;
            for idx in 0..merged_runs_count {
                let run_1_index = idx * 2;
                let run_2_index = idx * 2 + 1;
                if run_2_index >= runs_count {
                    let last_run_path = format!("{}/run_{}.txt", self.input_file_path, run_1_index);
                    let output_run_path = format!(
                        "{}/merge_pass_{}/run_{}.txt",
                        self.output_file_path,
                        self.merge_pass_count,
                        run_1_index / 2
                    );
                    fs::create_dir_all(format!(
                        "{}/merge_pass_{}",
                        self.output_file_path, self.merge_pass_count
                    ))
                    .expect("Unable to create output directory");
                    fs::copy(&last_run_path, &output_run_path)
                        .expect("Unable to copy last run file");
                    println!(
                        "> 复制未归并的最后一个顺串文件：{} 到 {}",
                        last_run_path, output_run_path
                    );
                    break;
                }
                println!("> 归并第 {} 和 第 {} 个顺串...", run_1_index, run_2_index);
                let run_1_path = format!("{}/run_{}.txt", self.input_file_path, run_1_index);
                let run_2_path = format!("{}/run_{}.txt", self.input_file_path, run_2_index);
                println!("> 归并文件路径：{} 和 {}", run_1_path, run_2_path);
                let output_run_path = format!(
                    "{}/merge_pass_{}/run_{}.txt",
                    self.output_file_path, self.merge_pass_count, idx
                );
                fs::create_dir_all(format!(
                    "{}/merge_pass_{}",
                    self.output_file_path, self.merge_pass_count
                ))
                .expect("Unable to create output directory");
                self.merge_two_runs(&run_1_path, &run_2_path, &output_run_path);
            }
            self.input_file_path = format!(
                "{}/merge_pass_{}",
                self.output_file_path, self.merge_pass_count
            );
            println!(
                "> 归并第 {} 次完成，下一次归并目录：{}",
                self.merge_pass_count, self.input_file_path
            );
            self.merge_pass_count += 1;
            if runs_count <= 2 {
                break;
            }
        }
    }
    pub fn merge_two_runs(&self, run_1_path: &str, run_2_path: &str, output_run_path: &str) {
        let file1 = File::open(run_1_path).expect("Unable to open run 1 file");
        let file2 = File::open(run_2_path).expect("Unable to open run 2 file");
        let reader1 = BufReader::new(file1);
        let reader2 = BufReader::new(file2);

        let mut iter1 = reader1
            .split(b' ')
            .filter_map(|res| res.ok())
            .filter_map(|bytes| String::from_utf8(bytes).ok())
            .filter_map(|s| s.trim().parse::<i32>().ok());
        let mut iter2 = reader2
            .split(b' ')
            .filter_map(|res| res.ok())
            .filter_map(|bytes| String::from_utf8(bytes).ok())
            .filter_map(|s| s.trim().parse::<i32>().ok());

        let mut output_file =
            File::create(output_run_path).expect("Unable to create output run file");
        let mut writer = BufWriter::new(&mut output_file);

        let mut val1 = iter1.next();
        let mut val2 = iter2.next();

        while val1.is_some() || val2.is_some() {
            match (val1, val2) {
                (Some(v1), Some(v2)) => {
                    if v1 <= v2 {
                        write!(writer, "{} ", v1).expect("Unable to write data");
                        val1 = iter1.next();
                    } else {
                        write!(writer, "{} ", v2).expect("Unable to write data");
                        val2 = iter2.next();
                    }
                }
                (Some(v1), None) => {
                    write!(writer, "{} ", v1).expect("Unable to write data");
                    val1 = iter1.next();
                }
                (None, Some(v2)) => {
                    write!(writer, "{} ", v2).expect("Unable to write data");
                    val2 = iter2.next();
                }
                (None, None) => break,
            }
        }
        println!("> 归并完成，输出文件路径：{}", output_run_path);
    }
}

pub fn run(run_length: u32) -> Duration {
    let run_generator = RunGenerator::new(
        run_length,
        "nums.txt".to_string(),
        "merge_passes/merge_pass_0".to_string(),
    );

    let mut merger = Merger::new(
        "merge_passes/merge_pass_0".to_string(),
        "merge_passes".to_string(),
    );

    let start_time = Instant::now();

    run_generator.generate_run_file();
    merger.merge();

    let end_time = Instant::now();

    let elapsed_time = end_time.duration_since(start_time);

    println!("> 排序耗时 {} 毫秒。", elapsed_time.as_millis());

    elapsed_time
}

pub fn evaluate(min_run_length: u32, max_run_length: u32, step: u32, n: u64) {
    //在data/project_2/origin_data.csv中记录不同run_length下的排序时间
    let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let output_file_path = format!("{}/data/project_2/origin_data.csv", cargo_manifest_dir);
    let file = File::create(&output_file_path).expect("Unable to create file");
    let mut writer = BufWriter::with_capacity(1024, file);
    writeln!(writer, "run_length,elapsed_time_ms").expect("Unable to write data");
    let source_generator = SourceFileGenerator::new(n, -1000, 1000, "nums.txt".to_string());
    source_generator.generate_file();
    for run_length in (min_run_length..=max_run_length).step_by(step as usize) {
        let elapsed_time = run(run_length);
        writeln!(writer, "{},{}", run_length, elapsed_time.as_millis())
            .expect("Unable to write data");
    }
    println!("> 评估数据已保存到 {}", output_file_path);
}
