use fs::*;
use io::*;
use rand::Rng;
use std::*;

pub enum Sequence {
    Sijk,
    Sikj,
    Sjik,
    Sjki,
    Skij,
    Skii,
}

#[derive(Clone, Debug)]
pub struct Address {
    pub tag: u32,
    pub index: u32,
    pub offset: u32,
}

#[derive(Clone, Debug)]
pub struct CacheLine {
    pub cache_line_size: u32,
    pub valid: bool,
    pub tag: u32,
    pub data: Vec<u32>,
}

#[derive(Clone, Debug)]
pub struct Cache {
    pub line_number: u32,
    pub lines: Vec<CacheLine>,
}

#[derive(Clone, Debug)]
pub struct Matrix {
    pub id: u32,
    pub dimension: u32,
    pub file_path: String,
    pub data: Vec<Vec<u32>>,
}

#[derive(Clone, Debug)]
pub struct Calculator {
    pub matrix_a: Matrix,
    pub matrix_b: Matrix,
    pub matrix_c: Matrix,
    pub cache: Cache,
    pub cache_miss: u32,
}

impl Matrix {
    pub fn new(id: u32, dimension: u32, file_path: &str) -> Matrix {
        let mut data = Vec::with_capacity(dimension as usize);
        let mut rng = rand::rng();
        println!("> 开始生成随机矩阵...");
        for _ in 0..dimension {
            let row: Vec<u32> = (0..dimension).map(|_| rng.random_range(0..100)).collect();
            data.push(row);
        }

        // 将矩阵写入文件
        let file = File::create(file_path).expect("无法创建文件");
        let mut writer = BufWriter::new(file);
        for row in &data {
            let line = row
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<String>>()
                .join(" ");
            writeln!(writer, "{}", line).expect("无法写入文件");
        }
        writer.flush().expect("无法刷新缓冲区");

        println!("> 随机矩阵生成完毕，已保存到文件: {}", file_path);

        // 打印矩阵
        println!("> 生成矩阵为:\n{:?}", data);

        Matrix {
            id,
            dimension,
            file_path: file_path.to_string(),
            data,
        }
    }

    pub fn from_file(id: u32, file_path: &str) -> Matrix {
        let file = File::open(file_path).expect("无法打开文件");
        let reader = BufReader::new(file);
        let mut data = Vec::new();
        for line in reader.lines() {
            let line = line.expect("无法读取行");
            let row: Vec<u32> = line
                .split_whitespace()
                .map(|v| v.parse().expect("无法解析数字"))
                .collect();
            data.push(row);
        }
        let dimension = data.len() as u32;
        Matrix {
            id,
            dimension,
            file_path: file_path.to_string(),
            data,
        }
    }

    pub fn data_to_file(&self) {
        let file = File::create(self.file_path.clone()).expect("无法创建文件");
        let mut writer = BufWriter::new(file);
        for row in &self.data {
            let line = row
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<String>>()
                .join(" ");
            writeln!(writer, "{}", line).expect("无法写入文件");
        }
        writer.flush().expect("无法刷新缓冲区");
        println!("> 矩阵已保存到文件: {}", self.file_path);
    }

    pub fn file_to_data(&mut self) {
        let file = File::open(&self.file_path).expect("无法打开文件");
        let reader = BufReader::new(file);
        self.data.clear();
        for line in reader.lines() {
            let line = line.expect("无法读取行");
            let row: Vec<u32> = line
                .split_whitespace()
                .map(|v| v.parse().expect("无法解析数字"))
                .collect();
            self.data.push(row);
        }
    }
}

impl CacheLine {
    pub fn new(cache_line_size: u32) -> CacheLine {
        CacheLine {
            cache_line_size,
            valid: false,
            tag: 0,
            data: vec![0; cache_line_size as usize],
        }
    }
}

impl Cache {
    pub fn new(line_number: u32, cache_line_size: u32) -> Cache {
        let mut lines = Vec::with_capacity(line_number as usize);
        for _ in 0..line_number {
            lines.push(CacheLine::new(cache_line_size));
        }
        Cache { line_number, lines }
    }
}

impl Calculator {
    pub fn new(matrix_a: Matrix, matrix_b: Matrix, cache: Cache, c_file_path: &str) -> Calculator {
        let dimension = matrix_a.dimension;
        let matrix_c = Matrix {
            id: 2,
            dimension,
            file_path: c_file_path.to_string(),
            data: vec![vec![0; dimension as usize]; dimension as usize],
        };
        Calculator {
            matrix_a,
            matrix_b,
            matrix_c,
            cache,
            cache_miss: 0,
        }
    }

    pub fn parse_address(&self, matrix: &Matrix, i: usize, j: usize) -> Address {
        // 元素的一维索引 = (i * dimension + j)
        let address = i * matrix.dimension as usize + j;
        // 偏移 = address % cache_line_size
        let offset = (address % (self.cache.lines[0].cache_line_size as usize)) as u32;
        // 索引 = (address / cache_line_size) % line_number
        let index = ((address / (self.cache.lines[0].cache_line_size as usize))
            % self.cache.line_number as usize) as u32;
        // 标签 = (address / cache_line_size) / line_number + matrix.id * line_number
        let tag = ((address / (self.cache.lines[0].cache_line_size as usize))
            / self.cache.line_number as usize) as u32
            + (matrix.id * self.cache.line_number) as u32;

        Address { tag, index, offset }
    }

    pub fn get_data(&mut self, matrix: &Matrix, i: usize, j: usize) -> Option<u32> {
        // 越界检查
        if i >= matrix.dimension as usize || j >= matrix.dimension as usize {
            return None;
        }
        // 解析地址
        let address = self.parse_address(matrix, i, j);
        let line = &mut self.cache.lines[address.index as usize];
        if line.valid && line.tag == address.tag {
            // Cache命中
            Some(line.data[address.offset as usize])
        } else {
            // Cache未命中
            self.cache_miss += 1;
            // 从矩阵中加载数据到Cache行
            line.valid = true;
            line.tag = address.tag;
            let start = (address.index * line.cache_line_size) as usize;
            for o in 0..line.cache_line_size as usize {
                let idx = start + o;
                let row = idx / matrix.dimension as usize;
                let col = idx % matrix.dimension as usize;
                if row < matrix.dimension as usize && col < matrix.dimension as usize {
                    line.data[o] = matrix.data[row][col];
                } else {
                    line.data[o] = 0; // 超出矩阵范围，填充0
                }
            }
            Some(line.data[address.offset as usize])
        }
    }

    pub fn calculate(&mut self, sequence: Sequence) {
        if self.matrix_a.dimension != self.matrix_b.dimension {
            panic!("矩阵维度不匹配，无法相乘");
        }
        let n = self.matrix_a.dimension as usize;
        println!("> 开始进行矩阵乘法计算...");
        match sequence {
            Sequence::Sijk => self.calculate_ijk(n),
            Sequence::Sikj => self.calculate_ikj(n),
            Sequence::Sjik => self.calculate_jik(n),
            Sequence::Sjki => self.calculate_jki(n),
            Sequence::Skij => self.calculate_kij(n),
            Sequence::Skii => self.calculate_kji(n),
        }
        println!("> 矩阵乘法计算完毕，计算结果为:\n{:?}", self.matrix_c.data);
        println!("> 计算过程中Cache未命中次数: {}", self.cache_miss);
        self.matrix_c.data_to_file();
    }

    fn calculate_ijk(&mut self, n: usize) {
        let temp_matrix_a = self.matrix_a.clone();
        let temp_matrix_b = self.matrix_b.clone();
        let mut temp_matrix_c = self.matrix_c.clone();

        for i in 0..n {
            for j in 0..n {
                for k in 0..n {
                    temp_matrix_c.data[i][j] += self.get_data(&temp_matrix_a, i, k).unwrap()
                        * self.get_data(&temp_matrix_b, k, j).unwrap();
                }
            }
        }

        self.matrix_c = temp_matrix_c;
        self.matrix_c.data_to_file();
    }

    fn calculate_ikj(&mut self, n: usize) {
        let matrix_a = &self.matrix_a;
        let matrix_b = &self.matrix_b;
        let matrix_c = &mut self.matrix_c;
        for i in 0..n {
            for k in 0..n {
                let a_ik = matrix_a.data[i][k];
                for j in 0..n {
                    matrix_c.data[i][j] += a_ik * matrix_b.data[k][j];
                }
            }
        }
    }
    fn calculate_jik(&mut self, n: usize) {
        let matrix_a = &self.matrix_a;
        let matrix_b = &self.matrix_b;
        let matrix_c = &mut self.matrix_c;
        for j in 0..n {
            for i in 0..n {
                let mut sum = 0;
                for k in 0..n {
                    sum += matrix_a.data[i][k] * matrix_b.data[k][j];
                }
                matrix_c.data[i][j] += sum;
            }
        }
    }
    fn calculate_jki(&mut self, n: usize) {
        let matrix_a = &self.matrix_a;
        let matrix_b = &self.matrix_b;
        let matrix_c = &mut self.matrix_c;
        for j in 0..n {
            for k in 0..n {
                let b_kj = matrix_b.data[k][j];
                for i in 0..n {
                    matrix_c.data[i][j] += matrix_a.data[i][k] * b_kj;
                }
            }
        }
    }
    fn calculate_kij(&mut self, n: usize) {
        let matrix_a = &self.matrix_a;
        let matrix_b = &self.matrix_b;
        let matrix_c = &mut self.matrix_c;
        for k in 0..n {
            for i in 0..n {
                let a_ik = matrix_a.data[i][k];
                for j in 0..n {
                    matrix_c.data[i][j] += a_ik * matrix_b.data[k][j];
                }
            }
        }
    }
    fn calculate_kji(&mut self, n: usize) {
        let matrix_a = &self.matrix_a;
        let matrix_b = &self.matrix_b;
        let matrix_c = &mut self.matrix_c;
        for k in 0..n {
            for j in 0..n {
                let b_kj = matrix_b.data[k][j];
                for i in 0..n {
                    matrix_c.data[i][j] += matrix_a.data[i][k] * b_kj;
                }
            }
        }
    }
}

