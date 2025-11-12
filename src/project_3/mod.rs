#![allow(unused)]
use fs::*;
use io::*;
use rand::Rng;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use std::*;

pub fn run() {
    let config = ExperimentConfig::default();
    if let Err(err) = ExperimentRunner::evaluate(config) {
        eprintln!("[project_3] Failed to complete evaluation: {}", err);
    }
}

fn resolve_manifest_relative_path(manifest_dir: &str, path: impl AsRef<Path>) -> PathBuf {
    let path = path.as_ref();
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        Path::new(manifest_dir).join(path)
    }
}

pub struct SourceFileGenerator {
    pub n: u64,
    pub min: i32,
    pub max: i32,
    pub output_file_path: PathBuf,
}

impl SourceFileGenerator {
    pub fn new(n: u64, min: i32, max: i32, output_file_path: String) -> Self {
        Self::with_output_dir(
            n,
            min,
            max,
            Path::new("data").join("project_3"),
            output_file_path,
        )
        .expect("Unable to prepare SourceFileGenerator")
    }

    pub fn with_output_dir(
        n: u64,
        min: i32,
        max: i32,
        output_dir: impl AsRef<Path>,
        file_name: impl AsRef<Path>,
    ) -> io::Result<Self> {
        let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
        let output_dir = resolve_manifest_relative_path(&cargo_manifest_dir, output_dir);
        fs::create_dir_all(&output_dir)?;
        let output_file_path = output_dir.join(file_name);
        Ok(Self {
            n,
            min,
            max,
            output_file_path,
        })
    }

    pub fn generate_file(&self) -> io::Result<()> {
        println!("> 开始生成随机数字序列文件...");
        if let Some(parent) = self.output_file_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = File::create(&self.output_file_path)?;
        let mut writer = BufWriter::with_capacity(1024, file);
        for _ in 0..self.n {
            let num = rand::rng().random_range(self.min..=self.max);
            write!(writer, "{} ", num)?;
        }
        println!(
            "> 随机数字序列文件生成完毕，文件路径：{}",
            self.output_file_path.display()
        );
        writer.flush()?;
        Ok(())
    }
}

pub struct LoserTree {
    pub losers: Vec<usize>,
    pub work_area: Vec<(i32, bool)>, // (value, is_frozen)
    pub k: usize,
}

impl LoserTree {
    pub fn new(initial_elements: Vec<i32>) -> Self {
        let k = initial_elements.len();

        if k == 0 {
            panic!("不能使用0个元素构建败者树。");
        }

        let mut tree = Self {
            // losers[0] 存最终胜者, losers[1..k] 存内部节点败者
            losers: vec![0; k], // 全部初始化为指向第一个元素
            work_area: initial_elements.into_iter().map(|v| (v, false)).collect(),
            k,
        };

        tree.build();

        tree
    }

    pub fn get_winner_idx(&self) -> usize {
        self.losers[0]
    }

    pub fn unfreeze_all_elements(&mut self) {
        for i in 0..self.k {
            if self.work_area[i].1 == true {
                self.work_area[i].1 = false;
            }
        }
    }

    pub fn build(&mut self) {
        // 假设所有内部节点都指向一个已知的“超级胜者”（例如索引0）
        let initial_winner_idx = 0;
        for i in 0..self.k {
            self.losers[i] = initial_winner_idx;
        }

        // 依次将所有 k 个 (现在已解冻的) 元素重新调整进树中
        for i in 0..self.k {
            self.replay_match(i);
        }
    }

    fn get_key(&self, idx: usize) -> i32 {
        let (value, is_frozen) = self.work_area[idx];

        if is_frozen { i32::MAX } else { value }
    }

    pub fn replace_and_replay(&mut self, leaf_idx: usize, new_element: (i32, bool)) {
        self.work_area[leaf_idx] = new_element;

        self.replay_match(leaf_idx);
    }

    /// 从指定的叶子节点开始，向上进行比赛
    fn replay_match(&mut self, leaf_idx: usize) {
        let mut winner_idx = leaf_idx;

        // p 是当前比赛发生的“内部节点”的索引
        // (k + i) / 2 将叶子节点(0..k-1)映射到树的下半部分
        let mut p = (self.k + leaf_idx) / 2;

        // 循环直到树根 (p == 0)
        while p > 0 {
            // 'loser_idx' 是存储在节点 p 的“旧败者”的索引
            let loser_idx = self.losers[p];

            if self.get_key(winner_idx) > self.get_key(loser_idx) {
                // 如果当前胜者 'winner_idx' 输了 (因为它值更大)

                // 将它(新的败者 'winner_idx') 存入 losers[p]
                self.losers[p] = winner_idx;

                // “旧的败者” 'loser_idx' 晋级，成为新的 'winner_idx'，
                winner_idx = loser_idx;
            }
            // 移动到父节点
            p /= 2;
        }

        self.losers[0] = winner_idx;
    }
}

pub struct InputElementReader {
    bytes: std::io::Bytes<BufReader<File>>,
}

impl InputElementReader {
    /// 创建一个新的读取器，指定一个大的内部缓冲区
    pub fn new(file: File) -> io::Result<Self> {
        let reader = BufReader::with_capacity(8 * 1024 * 1024, file);
        Ok(Self {
            bytes: reader.bytes(),
        })
    }

    /// 返回 Option<i32>，模拟迭代器
    pub fn next_element(&mut self) -> io::Result<Option<i32>> {
        let mut num_buf = Vec::new();
        let mut found_digit_or_sign = false;

        // 1. 跳过所有开头的空白字符
        let mut first_byte = None;
        loop {
            match self.bytes.next() {
                Some(Ok(b)) => {
                    if !b.is_ascii_whitespace() {
                        // 找到了数字（或负号）的第一个字节
                        first_byte = Some(b);
                        break;
                    }
                    // else: 是空白，继续跳过
                }
                Some(Err(e)) => return Err(e), // I/O 错误
                None => return Ok(None),       // 正常的文件末尾 (EOF)
            }
        }

        // 2. 收集数字的剩余字节
        if let Some(b) = first_byte {
            num_buf.push(b);
            found_digit_or_sign = true;

            loop {
                match self.bytes.next() {
                    Some(Ok(b)) => {
                        if b.is_ascii_whitespace() {
                            // 数字结束
                            break;
                        }
                        // else: 是数字的一部分
                        num_buf.push(b);
                    }
                    Some(Err(e)) => return Err(e), // I/O 错误
                    None => break,                 // 正常 EOF，这是最后一个数字
                }
            }
        }

        // 3. 如果我们找到了字节，就解析它们
        if found_digit_or_sign {
            // 将字节 [50, 51, 52] 转换为字符串 "123"
            let s = std::str::from_utf8(&num_buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            // 将字符串 "123" 解析为 i32
            s.parse::<i32>()
                .map(Some)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        } else {
            // 只找到了空白
            Ok(None)
        }
    }
}

pub struct RunGenerator {
    pub input_file_path: PathBuf,
    pub runs_dir: PathBuf,
    pub output_file_path: PathBuf,
    pub buffer_r: InputElementReader,
    pub buffer_w1: BufWriter<File>,
    pub buffer_w2: BufWriter<File>,
    pub run_count: u64,
    pub loser_tree: LoserTree,
}

impl RunGenerator {
    pub fn new(input_file_path: String, k: usize) -> Self {
        Self::with_dirs(
            Path::new("data").join("project_3").join(input_file_path),
            Path::new("data").join("project_3").join("runs"),
            k,
        )
        .expect("Failed to create RunGenerator")
    }

    pub fn with_dirs(
        input_file_path: impl AsRef<Path>,
        runs_dir: impl AsRef<Path>,
        k: usize,
    ) -> io::Result<Self> {
        let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
        let input_file_path = resolve_manifest_relative_path(&cargo_manifest_dir, input_file_path);
        let runs_dir = resolve_manifest_relative_path(&cargo_manifest_dir, runs_dir);
        fs::create_dir_all(&runs_dir)?;

        let mut buffer_r = InputElementReader::new(File::open(&input_file_path)?)?;

        let mut initial_elements = Vec::with_capacity(k);
        for _ in 0..k {
            initial_elements.push(buffer_r.next_element()?.unwrap_or(i32::MAX));
        }

        let output_file_path = runs_dir.join("run_0.txt");
        let buffer_w1 = BufWriter::with_capacity(1024, File::create(&output_file_path)?);
        let buffer_w2 = BufWriter::with_capacity(1024, File::create(&output_file_path)?);
        let loser_tree = LoserTree::new(initial_elements);
        Ok(Self {
            input_file_path,
            runs_dir,
            output_file_path,
            buffer_r,
            buffer_w1,
            buffer_w2,
            run_count: 0,
            loser_tree,
        })
    }

    fn update_output_file_path(&mut self, w: usize) -> io::Result<()> {
        self.output_file_path = self.runs_dir.join(format!("run_{}.txt", self.run_count));
        if w == 1 {
            self.buffer_w1.flush()?;
            self.buffer_w1 = BufWriter::with_capacity(1024, File::create(&self.output_file_path)?);
        } else {
            self.buffer_w2.flush()?;
            self.buffer_w2 = BufWriter::with_capacity(1024, File::create(&self.output_file_path)?);
        };
        self.run_count += 1;
        Ok(())
    }

    pub fn clean_directory_contents(dir_path: &Path) -> io::Result<()> {
        // 检查路径是否存在
        if !dir_path.exists() {
            // 如果目录本就不存在，那它就是“干净”的，直接返回 Ok
            return Ok(());
        }

        // 确保路径是一个目录
        if !dir_path.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("提供的路径不是一个目录: {}", dir_path.display()),
            ));
        }

        // 遍历目录中的所有条目
        for entry in fs::read_dir(dir_path)? {
            let entry = entry?; // 处理读取条目时的错误
            let path = entry.path();

            // 检查条目类型
            if path.is_dir() {
                // 如果是子目录，使用 remove_dir_all 递归删除
                fs::remove_dir_all(&path)?;
            } else {
                // 如果是文件，使用 remove_file 删除
                fs::remove_file(&path)?;
            }
        }

        Ok(())
    }

    pub fn generate_run_file(&mut self) -> io::Result<()> {
        RunGenerator::clean_directory_contents(&self.runs_dir)?;

        let mut pre_winner_value = i32::MIN;
        let mut all_elements_processed = false;

        while !all_elements_processed {
            let winner_idx = self.loser_tree.get_winner_idx();
            let winner_key = self.loser_tree.get_key(winner_idx); // 使用 get_key() 获取“有效”键值

            if winner_key == i32::MAX {
                self.update_output_file_path(if self.run_count % 2 == 0 { 1 } else { 2 })?;
                self.loser_tree.unfreeze_all_elements();
                self.loser_tree.build();
                pre_winner_value = i32::MIN;
                let new_winner_key = self.loser_tree.get_key(self.loser_tree.get_winner_idx());
                if new_winner_key == i32::MAX {
                    all_elements_processed = true; // 退出主循环
                }
                continue;
            }

            if self.run_count % 2 == 0 {
                write!(self.buffer_w1, "{} ", winner_key)?;
            } else {
                write!(self.buffer_w2, "{} ", winner_key)?;
            }
            pre_winner_value = winner_key;

            let next_element = self
                .buffer_r
                .next_element()? // 文件读完，用 MAX 填充
                .unwrap_or(i32::MAX);

            let replacement: (i32, bool);
            if next_element < pre_winner_value {
                replacement = (next_element, true);
            } else {
                replacement = (next_element, false);
            }

            self.loser_tree.replace_and_replay(winner_idx, replacement);
        }

        if self.run_count % 2 == 0 {
            self.buffer_w1.flush()?;
        } else {
            self.buffer_w2.flush()?;
        }

        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct MergeNode {
    pub weight: u64,
    pub leaf_id: Option<u32>,
    pub left: Option<Box<MergeNode>>,
    pub right: Option<Box<MergeNode>>,
}

impl Ord for MergeNode {
    fn cmp(&self, other: &Self) -> Ordering {
        other.weight.cmp(&self.weight)
    }
}

impl PartialOrd for MergeNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct Merger {
    pub input_file_path: String,
    pub output_file_path: String,
    pub merge_plan: Option<Box<MergeNode>>,
}

impl Merger {
    pub fn new(input_file_path: String, output_file_path: String) -> Self {
        let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
        let input_file_path = format!("{}/data/project_3/{}", cargo_manifest_dir, input_file_path);
        let output_file_path =
            format!("{}/data/project_3/{}", cargo_manifest_dir, output_file_path);
        let merge_pass_count = 1;
        Self {
            input_file_path,
            output_file_path,
            merge_plan: None,
        }
    }
    pub fn build_merge_plan(&mut self) -> io::Result<()> {
        println!("Building merge plan from: {}", self.input_file_path);

        let mut heap: BinaryHeap<Box<MergeNode>> = BinaryHeap::new();

        for entry in fs::read_dir(&self.input_file_path)? {
            let entry = entry?;
            let file_name = entry.file_name().to_string_lossy().to_string();

            if let Some(run_id) = parse_run_id(&file_name) {
                let file_path = format!("{}/{}", self.input_file_path, file_name);

                let file = File::open(file_path)?;
                let mut reader = InputElementReader::new(file)?;
                let mut count: u64 = 0;
                while reader.next_element()?.is_some() {
                    count += 1;
                }

                if count > 0 {
                    println!(
                        "Found run: {} (ID: {}, Length: {})",
                        file_name, run_id, count
                    );
                    let leaf_node = Box::new(MergeNode {
                        weight: count,
                        leaf_id: Some(run_id),
                        left: None,
                        right: None,
                    });
                    heap.push(leaf_node);
                }
            }
        }

        println!("Building merge tree ({} leaves)", heap.len());

        while heap.len() > 1 {
            // 弹出两个权重最小的 (因为我们反转了 Ord)
            let node1 = heap.pop().unwrap();
            let node2 = heap.pop().unwrap();

            let new_weight = node1.weight + node2.weight;

            // 创建新的内部节点
            let internal_node = Box::new(MergeNode {
                weight: new_weight,
                leaf_id: None, // 这是内部节点, 没有 leaf_id
                left: Some(node1),
                right: Some(node2),
            });

            // 将新节点推回堆中
            heap.push(internal_node);
        }

        // 循环结束, 堆中只剩一个根节点, 这就是完整的合并计划
        println!(
            "Merge plan complete. Total weight: {}",
            heap.peek().map_or(0, |n| n.weight)
        );
        self.merge_plan = heap.pop();

        Ok(())
    }

    pub fn merge_loop(&self) -> io::Result<()> {
        let root_node = match &self.merge_plan {
            Some(plan) => plan,
            None => {
                let err_msg = "没有合并计划。请先调用 build_merge_plan()。";
                return Err(io::Error::new(io::ErrorKind::NotFound, err_msg));
            }
        };

        // 准备临时目录 ".../data/project_3/temp"
        let base_path = self.input_file_path.strip_suffix("/runs").ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "无效的 input_file_path 格式")
        })?;

        let temp_dir_path = format!("{}/temp", base_path);

        // 清理并(重新)创建临时目录
        let _ = fs::remove_dir_all(&temp_dir_path); // 忽略清理失败 (可能目录不存在)
        fs::create_dir_all(&temp_dir_path)?;

        println!("开始合并... 临时目录: {}", temp_dir_path);

        // 启动递归合并
        let mut temp_file_counter: u32 = 1;
        let final_file_path =
            self.execute_merge_node(root_node, &temp_dir_path, &mut temp_file_counter)?;

        println!("合并完成。最终文件: {}", final_file_path);

        // 将最终合并的文件移动到目标输出位置
        fs::rename(final_file_path, &self.output_file_path)?;
        println!("已将最终文件移动到: {}", self.output_file_path);

        // 清理临时目录
        fs::remove_dir_all(temp_dir_path)?;
        println!("已清理临时目录。");

        Ok(())
    }

    fn execute_merge_node(
        &self,
        node: &Box<MergeNode>,
        temp_dir: &str,
        next_temp_id: &mut u32,
    ) -> io::Result<String> {
        // 如果是叶子节点，它代表一个原始的 run 文件。
        if let Some(run_id) = node.leaf_id {
            let file_path = format!("{}/run_{}.txt", self.input_file_path, run_id);
            return Ok(file_path);
        }

        // 否则，它是一个内部节点，需要合并其子节点
        if let (Some(left), Some(right)) = (&node.left, &node.right) {
            // 递归处理左子树 (获取左侧输入文件路径)
            let left_file_path = self.execute_merge_node(left, temp_dir, next_temp_id)?;

            // 递归处理右子树 (获取右侧输入文件路径)
            let right_file_path = self.execute_merge_node(right, temp_dir, next_temp_id)?;

            // 定义本次合并的输出文件路径
            let output_path = format!("{}/temp_{}.txt", temp_dir, *next_temp_id);
            *next_temp_id += 1; // 增加计数器

            println!(
                "  Merging: {} + {} -> {}",
                self.get_simple_path(&left_file_path),
                self.get_simple_path(&right_file_path),
                self.get_simple_path(&output_path)
            );

            // 执行双路合并
            self.perform_2_way_merge(&left_file_path, &right_file_path, &output_path)?;

            // 清理临时的输入文件
            if left_file_path.starts_with(temp_dir) {
                fs::remove_file(left_file_path)?;
            }
            if right_file_path.starts_with(temp_dir) {
                fs::remove_file(right_file_path)?;
            }

            // 返回新创建的临时文件的路径
            return Ok(output_path);
        }

        // 如果节点既不是叶子也不是有效的内部节点, 则出错
        Err(io::Error::new(io::ErrorKind::InvalidData, "无效的合并节点"))
    }

    fn perform_2_way_merge(
        &self,
        in_path_1: &str,
        in_path_2: &str,
        out_path: &str,
    ) -> io::Result<()> {
        let file1 = File::open(in_path_1)?;
        let file2 = File::open(in_path_2)?;
        let mut reader1 = InputElementReader::new(file1)?;
        let mut reader2 = InputElementReader::new(file2)?;

        let out_file = File::create(out_path)?;
        // 为输出也使用一个大的缓冲区
        let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, out_file);

        let mut elem1 = reader1.next_element()?;
        let mut elem2 = reader2.next_element()?;

        // 这是经典的合并排序 (merge) 逻辑
        loop {
            match (elem1, elem2) {
                // 情况 1: 两个文件都有元素
                (Some(val1), Some(val2)) => {
                    if val1 <= val2 {
                        // 写入 val1, 并从 reader1 读取下一个
                        write!(writer, "{} ", val1)?;
                        elem1 = reader1.next_element()?;
                    } else {
                        // 写入 val2, 并从 reader2 读取下一个
                        write!(writer, "{} ", val2)?;
                        elem2 = reader2.next_element()?;
                    }
                }
                // 情况 2: 只有 reader1 还有元素
                (Some(val1), None) => {
                    write!(writer, "{} ", val1)?;
                    elem1 = reader1.next_element()?;
                }
                // 情况 3: 只有 reader2 还有元素
                (None, Some(val2)) => {
                    write!(writer, "{} ", val2)?;
                    elem2 = reader2.next_element()?;
                }
                // 情况 4: 两个文件都已耗尽
                (None, None) => {
                    break;
                }
            }
        }

        writer.flush()?;
        Ok(())
    }

    fn get_simple_path<'a>(&self, path: &'a str) -> &'a str {
        path.rsplit('/').next().unwrap_or(path)
    }
}

#[derive(Debug, Clone)]
pub struct RunLengthEntry {
    pub run_id: u32,
    pub length: u64,
}

#[derive(Debug, Clone)]
pub struct RunStatsSummary {
    pub run_count: usize,
    pub total_length: u64,
    pub min_length: u64,
    pub max_length: u64,
    pub avg_length: f64,
}

pub struct RunStatistics {
    pub entries: Vec<RunLengthEntry>,
}

impl RunStatistics {
    pub fn from_directory(dir: &Path) -> io::Result<Self> {
        let mut entries = Vec::new();
        if !dir.exists() {
            return Ok(Self { entries });
        }

        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            if let Some(run_id) = parse_run_id(&file_name) {
                let length = count_elements_in_file(&entry.path())?;
                entries.push(RunLengthEntry { run_id, length });
            }
        }
        entries.sort_by_key(|entry| entry.run_id);
        Ok(Self { entries })
    }

    pub fn summary(&self) -> Option<RunStatsSummary> {
        if self.entries.is_empty() {
            return None;
        }
        let mut total = 0_u64;
        let mut min_length = self.entries[0].length;
        let mut max_length = self.entries[0].length;
        for entry in &self.entries {
            total += entry.length;
            if entry.length < min_length {
                min_length = entry.length;
            }
            if entry.length > max_length {
                max_length = entry.length;
            }
        }
        Some(RunStatsSummary {
            run_count: self.entries.len(),
            total_length: total,
            min_length,
            max_length,
            avg_length: total as f64 / self.entries.len() as f64,
        })
    }

    pub fn write_report<P: AsRef<Path>>(&self, output_path: P) -> io::Result<()> {
        let mut file = File::create(output_path)?;
        writeln!(file, "run_id,run_length")?;
        for entry in &self.entries {
            writeln!(file, "{},{}", entry.run_id, entry.length)?;
        }
        Ok(())
    }
}

fn count_elements_in_file(path: &Path) -> io::Result<u64> {
    let file = File::open(path)?;
    let mut reader = InputElementReader::new(file)?;
    let mut count = 0_u64;
    while reader.next_element()?.is_some() {
        count += 1;
    }
    Ok(count)
}

pub struct MergePlanSummary {
    pub merge_steps: Vec<String>,
    pub leaf_count: usize,
    pub max_depth: u32,
    pub weighted_path_len: u64,
}

impl MergePlanSummary {
    pub fn from_root(node: &MergeNode) -> Self {
        let mut summary = Self {
            merge_steps: Vec::new(),
            leaf_count: 0,
            max_depth: 0,
            weighted_path_len: 0,
        };
        summary.collect(node, 0);
        summary
    }

    fn collect(&mut self, node: &MergeNode, depth: u32) {
        self.max_depth = self.max_depth.max(depth);

        if node.leaf_id.is_some() {
            self.leaf_count += 1;
            self.weighted_path_len += node.weight * depth as u64;
            return;
        }

        if let (Some(left), Some(right)) = (&node.left, &node.right) {
            self.collect(left, depth + 1);
            self.collect(right, depth + 1);
            self.merge_steps.push(format!(
                "{} + {} -> {}",
                Self::describe(left),
                Self::describe(right),
                Self::describe(node)
            ));
        }
    }

    fn describe(node: &MergeNode) -> String {
        match node.leaf_id {
            Some(id) => format!("run_{}(len={})", id, node.weight),
            None => format!("temp(len={})", node.weight),
        }
    }

    pub fn write_report<P: AsRef<Path>>(&self, output_path: P) -> io::Result<()> {
        let mut file = File::create(output_path)?;
        writeln!(file, "# Merge Plan Summary")?;
        writeln!(file, "leaf_count: {}", self.leaf_count)?;
        writeln!(file, "max_depth: {}", self.max_depth)?;
        writeln!(file, "weighted_path_length: {}", self.weighted_path_len)?;
        writeln!(file, "")?;
        writeln!(file, "steps:")?;
        for (idx, step) in self.merge_steps.iter().enumerate() {
            writeln!(file, "{}. {}", idx + 1, step)?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ExperimentConfig {
    pub total_numbers: u64,
    pub min_value: i32,
    pub max_value: i32,
    pub k_values: Vec<usize>,
    pub input_file: String,
    pub runs_dir: String,
    pub sorted_output_file: String,
    pub summary_csv: String,
    pub run_stats_dir: String,
    pub merge_plan_dir: String,
}

impl Default for ExperimentConfig {
    fn default() -> Self {
        Self {
            total_numbers: 20_000,
            min_value: -1_000,
            max_value: 1_000,
            k_values: vec![8, 16, 32, 64],
            input_file: "nums.txt".into(),
            runs_dir: "runs".into(),
            sorted_output_file: "sorted_nums.txt".into(),
            summary_csv: "origin_data.csv".into(),
            run_stats_dir: "analysis/run_stats".into(),
            merge_plan_dir: "analysis/merge_plans".into(),
        }
    }
}

pub struct ExperimentRunner;

impl ExperimentRunner {
    pub fn evaluate(config: ExperimentConfig) -> io::Result<()> {
        let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
        let base_dir = format!("{}/data/project_3", cargo_manifest_dir);

        let runs_dir_path = Path::new(&base_dir).join(&config.runs_dir);
        let run_stats_dir_path = Path::new(&base_dir).join(&config.run_stats_dir);
        let merge_plan_dir_path = Path::new(&base_dir).join(&config.merge_plan_dir);
        let summary_csv_path = Path::new(&base_dir).join(&config.summary_csv);

        fs::create_dir_all(&runs_dir_path)?;
        fs::create_dir_all(&run_stats_dir_path)?;
        fs::create_dir_all(&merge_plan_dir_path)?;

        let mut summary_writer = BufWriter::new(File::create(&summary_csv_path)?);
        writeln!(
            summary_writer,
            "k,run_count,total_numbers,min_run_length,max_run_length,avg_run_length,total_time_ms,max_tree_depth,weighted_path_length"
        )?;

        let source_generator = SourceFileGenerator::new(
            config.total_numbers,
            config.min_value,
            config.max_value,
            config.input_file.clone(),
        );
        source_generator.generate_file()?;

        for &k in &config.k_values {
            if k == 0 {
                continue;
            }

            RunGenerator::clean_directory_contents(&runs_dir_path)?;

            let mut run_generator = RunGenerator::new(config.input_file.clone(), k);
            let mut merger =
                Merger::new(config.runs_dir.clone(), config.sorted_output_file.clone());

            let start_time = Instant::now();
            run_generator.generate_run_file()?;

            let run_stats = RunStatistics::from_directory(&runs_dir_path)?;
            let run_stats_file = run_stats_dir_path.join(format!("k_{}.csv", k));
            run_stats.write_report(run_stats_file)?;

            merger.build_merge_plan()?;
            let plan_summary = merger
                .merge_plan
                .as_ref()
                .map(|node| MergePlanSummary::from_root(node))
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "merge plan is empty"))?;
            let plan_report = merge_plan_dir_path.join(format!("k_{}.txt", k));
            plan_summary.write_report(plan_report)?;

            merger.merge_loop()?;
            let elapsed_ms = start_time.elapsed().as_millis();

            if let Some(summary) = run_stats.summary() {
                writeln!(
                    summary_writer,
                    "{},{},{},{},{},{:.2},{},{},{}",
                    k,
                    summary.run_count,
                    summary.total_length,
                    summary.min_length,
                    summary.max_length,
                    summary.avg_length,
                    elapsed_ms,
                    plan_summary.max_depth,
                    plan_summary.weighted_path_len
                )?;
            } else {
                writeln!(
                    summary_writer,
                    "{},{},{},{},{},{:.2},{},{},{}",
                    k,
                    0,
                    0,
                    0,
                    0,
                    0.0,
                    elapsed_ms,
                    plan_summary.max_depth,
                    plan_summary.weighted_path_len
                )?;
            }
        }

        summary_writer.flush()?;
        Ok(())
    }
}

pub fn parse_run_id(file_name: &str) -> Option<u32> {
    file_name
        .strip_prefix("run_")?
        .strip_suffix(".txt")?
        .parse::<u32>()
        .ok()
}
