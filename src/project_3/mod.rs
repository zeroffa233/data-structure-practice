#![allow(unused)]
use fs::*;
use io::*;
use rand::Rng;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::path::Path;
use std::time::{Duration, Instant};
use std::*;

pub fn run() {
    let mut generator = SourceFileGenerator::new(100, 0, 100, "nums.txt".into());
    generator.generate_file();
    let mut run_generator = RunGenerator::new("nums.txt".into(), 16);
    run_generator.generate_run_file();
    let mut merger = Merger::new("runs".into(), "sorted_nums.txt".into());
    merger
        .build_merge_plan()
        .expect("Failed to build merge plan");
    merger.merge_loop().expect("Failed to merge runs");
}

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
            // ls[0] 存最终胜者, ls[1..k] 存内部节点败者
            // 为了简化索引计算 (p/2)，我们通常让 ls 的大小为 k
            // ls[0] 是胜者
            // ls[1]..ls[k-1] 是 k-1 个内部节点
            losers: vec![0; k], // 全部初始化为指向第一个元素
            work_area: initial_elements.into_iter().map(|v| (v, false)).collect(),
            k,
        };

        tree.build(); // 构建败者树

        tree
    }

    /// 获取最终胜者的索引 (即 losers[0])
    /// 外部通过这个索引从 work_area[idx] 获取胜者元素
    pub fn get_winner_idx(&self) -> usize {
        self.losers[0]
    }

    pub fn unfreeze_all_elements(&mut self) {
        for i in 0..self.k {
            // self.work_area[i] 是 (i32, bool)
            // .1 是 is_frozen 标志
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
        // self.work_area[idx] 是一个 (i32, bool) 元组
        // .0 是真实值, .1 是 is_frozen 标志
        let (value, is_frozen) = self.work_area[idx];

        if is_frozen { i32::MAX } else { value }
    }
    pub fn replace_and_replay(&mut self, leaf_idx: usize, new_element: (i32, bool)) {
        // 1. 替换工作区中的元素
        self.work_area[leaf_idx] = new_element;

        // 2. 开始“重赛” (Replay)
        self.replay_match(leaf_idx);
    }

    /// 从指定的叶子节点开始，向上进行比赛 (私有辅助方法)
    /// - `leaf_idx`: 刚刚被更新的叶子节点的索引
    fn replay_match(&mut self, leaf_idx: usize) {
        // 'winner_idx' 是当前晋级的“胜者”的索引 (初始为刚更新的叶子)
        let mut winner_idx = leaf_idx;

        // 'p' 是当前比赛发生的“内部节点”的索引
        // (k + i) / 2 是将叶子节点(0..k-1)映射到树的下半部分(内部节点)的巧妙方法
        let mut p = (self.k + leaf_idx) / 2;

        // 循环直到树根 (p == 0)
        while p > 0 {
            // 'loser_idx' 是存储在节点 p 的“旧败者”的索引
            let loser_idx = self.losers[p];

            // [修改点]
            // 比赛：W[winner_idx] vs W[loser_idx]
            // 我们使用 get_key 辅助函数来获取“有效”键值

            // 检查：如果当前晋级的 'winner_idx' 的 *有效键值*
            // 大于
            // 存储在内部节点 'p' 的“败者” 'loser_idx' 的 *有效键值*
            if self.get_key(winner_idx) > self.get_key(loser_idx) {
                // 如果当前胜者 'winner_idx' 输了 (因为它值更大)

                // 1. 将它(新的败者 'winner_idx') 存入 losers[p]
                self.losers[p] = winner_idx;

                // 2. “旧的败者” 'loser_idx' 晋级，成为新的 'winner_idx'，
                //    它将去参与上一层的比赛
                winner_idx = loser_idx;
            } else {
                // 如果当前胜者 'winner_idx' 赢了 (值更小或相等)

                // 1. 'loser_idx' 仍然是败者，losers[p] 不变
                // 2. 'winner_idx' 继续晋级，去参与上一层的比赛
            }

            // 移动到父节点
            p /= 2;
        }

        // 循环结束, p == 0, 'winner_idx' 是经历了一路比赛的“最终胜者”
        // 将其存入 losers[0]
        self.losers[0] = winner_idx;
    }
}

pub struct InputElementReader {
    bytes: std::io::Bytes<BufReader<File>>,
}

impl InputElementReader {
    /// 创建一个新的读取器，指定一个大的内部缓冲区
    pub fn new(file: File) -> io::Result<Self> {
        // 关键：设置一个大的缓冲区，例如 8MB
        let reader = BufReader::with_capacity(8 * 1024 * 1024, file);
        Ok(Self {
            bytes: reader.bytes(),
        })
    }

    /// 这是败者树算法调用的核心方法
    /// 它返回 Option<i32>，模拟迭代器
    /// 它会自动跳过数字之间的任意空白（空格、换行、Tab等）
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
    pub input_file_path: String,
    pub output_file_path: String,
    pub buffer_r: InputElementReader,
    pub buffer_w1: BufWriter<File>,
    pub buffer_w2: BufWriter<File>,
    pub run_count: u64,
    pub loser_tree: LoserTree,
}

impl RunGenerator {
    pub fn new(input_file_path: String, k: usize) -> Self {
        let run_count = 0;
        let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
        let input_file_path = format!("{}/data/project_3/{}", cargo_manifest_dir, input_file_path);
        let output_file_path = format!(
            "{}/data/project_3/runs/run_{}.txt",
            cargo_manifest_dir, run_count
        );
        let mut buffer_r =
            InputElementReader::new(File::open(&input_file_path).expect("Unable to open file"))
                .expect("Failed to create InputElementReader");

        let mut initial_elements = Vec::with_capacity(k);
        for _ in 0..k {
            initial_elements.push(
                buffer_r
                    .next_element()
                    .expect("Read error")
                    .unwrap_or(i32::MAX),
            );
        }

        let mut buffer_w1 = BufWriter::with_capacity(
            1024,
            File::create(&output_file_path).expect("Unable to create file"),
        );
        let mut buffer_w2 = BufWriter::with_capacity(
            1024,
            File::create(&output_file_path).expect("Unable to create file"),
        );
        let loser_tree = LoserTree::new(initial_elements);
        Self {
            input_file_path,
            output_file_path,
            buffer_r,
            buffer_w1,
            buffer_w2,
            run_count,
            loser_tree,
        }
    }

    fn update_output_file_path(&mut self, w: usize) {
        let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
        self.output_file_path = format!(
            "{}/data/project_3/runs/run_{}.txt",
            cargo_manifest_dir, self.run_count
        );
        if w == 1 {
            self.buffer_w1.flush().expect("Unable to flush buffer");
            self.buffer_w1 = BufWriter::with_capacity(
                1024,
                File::create(&self.output_file_path).expect("Unable to create file"),
            );
        } else {
            self.buffer_w1.flush().expect("Unable to flush buffer");
            self.buffer_w2 = BufWriter::with_capacity(
                1024,
                File::create(&self.output_file_path).expect("Unable to create file"),
            );
        };
        self.run_count += 1;
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

    pub fn generate_run_file(&mut self) {
        let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
        let merge_pass_dir = format!("{}/data/project_3/runs/", cargo_manifest_dir);
        let merge_pass_path = Path::new(&merge_pass_dir);
        RunGenerator::clean_directory_contents(merge_pass_path)
            .expect("Failed to clean merge_pass_0 directory");

        // [1] 初始化：pre_winner_value 设为最小值
        let mut pre_winner_value = i32::MIN;

        // [2] 循环标志
        let mut all_elements_processed = false;

        while !all_elements_processed {
            // [3] 获取胜者
            let winner_idx = self.loser_tree.get_winner_idx();
            let winner_key = self.loser_tree.get_key(winner_idx); // 使用 get_key() 获取“有效”键值

            // [4] 检查：当前顺串是否结束？
            if winner_key == i32::MAX {
                // 信号：所有 k 个槽位都已被冻结。当前顺串结束。

                // 4.1 切换输出文件 (flush, close, open new)
                // 假设 update_output_file_path 会 flush *旧* 的 buffer
                self.update_output_file_path(if self.run_count % 2 == 0 { 1 } else { 2 });

                // 4.2 取消所有元素的冻结
                self.loser_tree.unfreeze_all_elements();

                // 4.3 [关键修复] 必须重建败者树！
                self.loser_tree.build();

                // 4.4 重置 pre_winner_value，为新顺串做准备
                pre_winner_value = i32::MIN;

                // 4.5 检查：是否 *所有* 数据都已处理完毕？
                // (如果重建后，胜者依然是 MAX，说明 work_area 里全是 MAX)
                let new_winner_key = self.loser_tree.get_key(self.loser_tree.get_winner_idx());
                if new_winner_key == i32::MAX {
                    all_elements_processed = true; // 退出主循环
                }

                continue; // 开始新顺串的下一次循环
            }

            // [5] (如果顺串未结束) 输出胜者
            // 此时 winner_key != MAX，它就是我们要输出的真实值
            if self.run_count % 2 == 0 {
                write!(self.buffer_w1, "{} ", winner_key).expect("Unable to write data");
            } else {
                write!(self.buffer_w2, "{} ", winner_key).expect("Unable to write data");
            }

            // [6] [关键修复] 更新 pre_winner_value 为刚刚输出的值
            pre_winner_value = winner_key;

            // [7] 读取下一个新元素
            let next_element = self
                .buffer_r
                .next_element()
                .expect("Read error")
                .unwrap_or(i32::MAX); // 文件读完，用 MAX 填充

            // [8] [关键修复] 核心逻辑：判断新元素是否需要冻结
            let replacement: (i32, bool);
            if next_element < pre_winner_value {
                // [Case B] 新元素太小，属于“下一个”顺串 -> 冻结
                replacement = (next_element, true);
            } else {
                // [Case A] 新元素OK，属于“当前”顺串 -> 不冻结
                replacement = (next_element, false);
            }

            // [9] 替换并重赛
            self.loser_tree.replace_and_replay(winner_idx, replacement);
        }

        // [10] [关键修复] 循环结束后，flush 最后一个缓冲区
        if self.run_count % 2 == 0 {
            self.buffer_w1
                .flush()
                .expect("Unable to flush final buffer");
        } else {
            self.buffer_w2
                .flush()
                .expect("Unable to flush final buffer");
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct MergeNode {
    /// 权重 (这个“堆”中元素的总数)
    pub weight: u64,

    /// 如果是叶子节点, 它是 Some(run_id), 例如 1 对应 "run_1.txt"
    pub leaf_id: Option<u32>,

    /// 如果是内部节点 (一次合并), 它有左右子节点
    pub left: Option<Box<MergeNode>>,
    pub right: Option<Box<MergeNode>>,
}

// 为 MergeNode 实现 Ord, 以便在 BinaryHeap (最小堆) 中使用
// 我们反转比较逻辑, 让 BinaryHeap (默认最大堆) 表现为最小堆
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

        // 1. 初始化最小堆
        let mut heap: BinaryHeap<Box<MergeNode>> = BinaryHeap::new();

        // 2. 遍历 runs 目录 (使用 String 路径)
        for entry in fs::read_dir(&self.input_file_path)? {
            let entry = entry?;
            let file_name = entry.file_name().to_string_lossy().to_string();

            // 3. 检查是否为 "run_X.txt" 文件
            if let Some(run_id) = parse_run_id(&file_name) {
                // 构造完整的文件路径 (使用 String)
                let file_path = format!("{}/{}", self.input_file_path, file_name);

                // 4. 打开文件并统计元素数量
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
                    // 5. 创建叶子节点并推入堆
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

        if heap.is_empty() {
            println!("Warning: No runs found to merge.");
            self.merge_plan = None;
            return Ok(());
        }

        println!("--- Building merge tree ({} leaves) ---", heap.len());

        // 6. 运行哈夫曼算法：不断合并最小的两个节点
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

        // 7. 循环结束, 堆中只剩一个根节点, 这就是完整的合并计划
        println!(
            "--- Merge plan complete. Total weight: {} ---",
            heap.peek().map_or(0, |n| n.weight)
        );
        self.merge_plan = heap.pop();

        Ok(())
    }
    pub fn merge_loop(&self) -> io::Result<()> {
        // 1. 检查合并计划是否存在
        let root_node = match &self.merge_plan {
            Some(plan) => plan,
            None => {
                let err_msg = "没有合并计划。请先调用 build_merge_plan()。";
                return Err(io::Error::new(io::ErrorKind::NotFound, err_msg));
            }
        };

        // 2. 准备临时目录 (例如 ".../data/project_3/temp")
        // 我们从 'input_file_path' 推断出 'base_path'
        let base_path = self.input_file_path.strip_suffix("/runs").ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "无效的 input_file_path 格式")
        })?;

        let temp_dir_path = format!("{}/temp", base_path);

        // 3. 清理并(重新)创建临时目录
        let _ = fs::remove_dir_all(&temp_dir_path); // 忽略清理失败 (可能目录不存在)
        fs::create_dir_all(&temp_dir_path)?;

        println!("开始合并... 临时目录: {}", temp_dir_path);

        // 4. 启动递归合并
        let mut temp_file_counter: u32 = 1;
        let final_file_path =
            self.execute_merge_node(root_node, &temp_dir_path, &mut temp_file_counter)?;

        println!("合并完成。最终文件: {}", final_file_path);

        // 5. 将最终合并的文件移动到目标输出位置
        fs::rename(final_file_path, &self.output_file_path)?;
        println!("已将最终文件移动到: {}", self.output_file_path);

        // 6. 清理临时目录
        fs::remove_dir_all(temp_dir_path)?;
        println!("已清理临时目录。");

        Ok(())
    }

    /// -------------------------------------------------------------
    /// 辅助方法 (1): 递归执行合并节点
    /// -------------------------------------------------------------
    ///
    /// 后序遍历哈夫曼树。
    /// - `node`: 当前要处理的节点。
    /// - `temp_dir`: 存储中间文件的目录。
    /// - `next_temp_id`: 用于生成唯一临时文件名的计数器。
    ///
    /// 返回值: `Result<String>`，代表此节点处理完毕后,
    ///         其“堆”所在的文件的路径。
    ///
    fn execute_merge_node(
        &self,
        node: &Box<MergeNode>,
        temp_dir: &str,
        next_temp_id: &mut u32,
    ) -> io::Result<String> {
        // --- 基本情况 (Base Case): 叶子节点 ---
        // 如果是叶子节点，它代表一个原始的 run 文件。
        // 我们不需要做任何事，只需返回该文件的路径。
        if let Some(run_id) = node.leaf_id {
            let file_path = format!("{}/run_{}.txt", self.input_file_path, run_id);
            return Ok(file_path);
        }

        // --- 递归情况 (Recursive Case): 内部节点 ---
        // 这是一个合并操作。
        if let (Some(left), Some(right)) = (&node.left, &node.right) {
            // 1. 递归处理左子树 (获取左侧输入文件路径)
            let left_file_path = self.execute_merge_node(left, temp_dir, next_temp_id)?;

            // 2. 递归处理右子树 (获取右侧输入文件路径)
            let right_file_path = self.execute_merge_node(right, temp_dir, next_temp_id)?;

            // 3. 定义本次合并的输出文件路径
            let output_path = format!("{}/temp_{}.txt", temp_dir, *next_temp_id);
            *next_temp_id += 1; // 增加计数器

            println!(
                "  Merging: {} + {} -> {}",
                self.get_simple_path(&left_file_path),
                self.get_simple_path(&right_file_path),
                self.get_simple_path(&output_path)
            );

            // 4. 执行双路合并
            self.perform_2_way_merge(&left_file_path, &right_file_path, &output_path)?;

            // 5. (重要) 清理临时的输入文件
            // 如果输入文件是临时文件 (而不是原始run), 则删除它们以节省磁盘空间
            if left_file_path.starts_with(temp_dir) {
                fs::remove_file(left_file_path)?;
            }
            if right_file_path.starts_with(temp_dir) {
                fs::remove_file(right_file_path)?;
            }

            // 6. 返回新创建的临时文件的路径
            return Ok(output_path);
        }

        // 如果节点既不是叶子也不是有效的内部节点, 则出错
        Err(io::Error::new(io::ErrorKind::InvalidData, "无效的合并节点"))
    }

    /// -------------------------------------------------------------
    /// 辅助方法 (2): 执行双路合并
    /// -------------------------------------------------------------
    ///
    /// 从两个已排序的输入文件读取, 将合并结果写入一个输出文件。
    ///
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
                    break; // 合并完成
                }
            }
        }

        writer.flush()?; // 确保所有缓冲的数据都写入磁盘
        Ok(())
    }

    /// 辅助方法 (3): 仅用于日志, 获取路径的最后一部分
    fn get_simple_path<'a>(&self, path: &'a str) -> &'a str {
        path.rsplit('/').next().unwrap_or(path)
    }
}

pub fn parse_run_id(file_name: &str) -> Option<u32> {
    file_name
        .strip_prefix("run_")?
        .strip_suffix(".txt")?
        .parse::<u32>()
        .ok()
}
