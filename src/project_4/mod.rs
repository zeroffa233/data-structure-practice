#![allow(unused)]

use std::cmp::Ordering;
use std::env;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

/// The default size (in number of elements) of each in-memory buffer.
const DEFAULT_BUFFER_CAPACITY: usize = 1024;

/// Configuration for the improved external merge sort experiment.
#[derive(Debug, Clone)]
pub struct Project4Config {
    /// Directory that stores all initial runs (`run_*.txt`).
    pub runs_dir: String,
    /// Path of the final sorted output file.
    pub output_file: String,
    /// Maximum number of runs to merge in a single pass.
    pub max_k: usize,
    /// Number of extra buffers besides the per-run primary buffers.
    pub extra_input_buffers: usize,
    /// Capacity of each buffer in number of integers.
    pub buffer_capacity: usize,
}

impl Default for Project4Config {
    fn default() -> Self {
        let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
        Self {
            runs_dir: format!("{}/data/project_4/runs", cargo_manifest_dir),
            output_file: format!("{}/data/project_4/sorted_output.txt", cargo_manifest_dir),
            max_k: 16,
            extra_input_buffers: 2,
            buffer_capacity: DEFAULT_BUFFER_CAPACITY,
        }
    }
}

/// Entry point used by `main` to demonstrate the project.
pub fn run() {
    let config = Project4Config::default();
    if let Err(err) = KWayLoserTreeMerger::new(config).and_then(|mut merger| merger.merge()) {
        eprintln!("[project_4] Merge failed: {}", err);
    }
}

/// Merge driver that coordinates loser-tree based k-way merging with buffer pooling.
pub struct KWayLoserTreeMerger {
    config: Project4Config,
    run_files: Vec<PathBuf>,
}

impl KWayLoserTreeMerger {
    pub fn new(config: Project4Config) -> io::Result<Self> {
        let mut run_files = Vec::new();
        let runs_dir = Path::new(&config.runs_dir);
        if runs_dir.exists() {
            for entry in fs::read_dir(runs_dir)? {
                let entry = entry?;
                if entry.file_type()?.is_file() {
                    run_files.push(entry.path());
                }
            }
        }
        run_files.sort();
        Ok(Self { config, run_files })
    }

    pub fn merge(&mut self) -> io::Result<()> {
        if self.run_files.is_empty() {
            println!("[project_4] No runs found in {}", self.config.runs_dir);
            return Ok(());
        }

        fs::create_dir_all(
            Path::new(&self.config.output_file)
                .parent()
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "invalid output path"))?,
        )?;

        // Determine how many runs to merge simultaneously.
        let active_k = usize::min(self.config.max_k, self.run_files.len());

        // Prepare buffer pool: one primary buffer per active run plus the configured extras.
        let mut buffer_pool = BufferPool::new(
            active_k + self.config.extra_input_buffers,
            self.config.buffer_capacity,
        );

        // Initialise run buffers.
        let mut run_buffers = Vec::with_capacity(active_k);
        for (idx, run_path) in self.run_files.iter().take(active_k).enumerate() {
            let file = File::open(run_path)?;
            let reader = InputElementReader::new(file)?;
            let mut run_buffer = RunBuffer::new(idx, reader, self.config.buffer_capacity, &mut buffer_pool)?;
            if run_buffer.is_finished() {
                // Skip empty runs but keep buffer bookkeeping consistent.
                buffer_pool.release(run_buffer.take_primary_buffer());
                continue;
            }
            run_buffers.push(run_buffer);
        }

        if run_buffers.is_empty() {
            println!("[project_4] All runs are empty");
            return Ok(());
        }

        // Build loser tree with currently active runs.
        let mut loser_tree = LoserTree::new(run_buffers.len());
        loser_tree.build(&run_buffers);

        buffer_pool.assign_extra_buffer(&mut run_buffers)?;

        let output_file = File::create(&self.config.output_file)?;
        let mut writer = BufWriter::new(output_file);

        loop {
            let winner_idx = loser_tree.winner();
            let winner_key = run_buffers[winner_idx].current_value();

            if winner_key.is_none() {
                break;
            }

            let value = winner_key.unwrap();
            write!(writer, "{} ", value)?;

            run_buffers[winner_idx].advance(&mut buffer_pool)?;
            buffer_pool.assign_extra_buffer(&mut run_buffers)?;
            loser_tree.replay(winner_idx, &run_buffers);

            if run_buffers.iter().all(RunBuffer::is_finished) {
                break;
            }
        }

        for run in run_buffers.iter_mut() {
            run.release_all_buffers(&mut buffer_pool);
        }

        writer.flush()?;
        Ok(())
    }
}

/// Simple buffer pool that recycles `Vec<i32>` allocations.
struct BufferPool {
    buffers: Vec<Vec<i32>>,
    capacity: usize,
}

impl BufferPool {
    fn new(buffer_count: usize, capacity: usize) -> Self {
        let mut buffers = Vec::with_capacity(buffer_count);
        for _ in 0..buffer_count {
            buffers.push(Vec::with_capacity(capacity));
        }
        Self { buffers, capacity }
    }

    fn acquire(&mut self) -> Vec<i32> {
        self.buffers
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(self.capacity))
    }

    fn release(&mut self, mut buffer: Vec<i32>) {
        buffer.clear();
        self.buffers.push(buffer);
    }

    fn available(&self) -> usize {
        self.buffers.len()
    }

    fn assign_extra_buffer(&mut self, run_buffers: &mut [RunBuffer]) -> io::Result<()> {
        if self.available() == 0 {
            return Ok(());
        }

        if let Some((idx, _)) = run_buffers
            .iter_mut()
            .enumerate()
            .filter(|(_, run)| !run.is_finished() && !run.has_secondary_buffer())
            .max_by_key(|(_, run)| run.refill_count())
        {
            let buffer = self.acquire();
            if let Some(buffer) = run_buffers[idx].try_prefetch(buffer)? {
                self.release(buffer);
            }
        }

        Ok(())
    }
}

/// Keeps track of buffered data for a single run.
struct RunBuffer {
    id: usize,
    reader: InputElementReader,
    primary: Vec<i32>,
    primary_pos: usize,
    secondary: Option<Vec<i32>>,
    buffer_capacity: usize,
    refill_count: u64,
    finished: bool,
}

impl RunBuffer {
    fn new(
        id: usize,
        mut reader: InputElementReader,
        buffer_capacity: usize,
        buffer_pool: &mut BufferPool,
    ) -> io::Result<Self> {
        let mut primary = buffer_pool.acquire();
        Self::fill_buffer(&mut reader, &mut primary, buffer_capacity)?;
        let finished = primary.is_empty();

        Ok(Self {
            id,
            reader,
            primary,
            primary_pos: 0,
            secondary: None,
            buffer_capacity,
            refill_count: if finished { 0 } else { 1 },
            finished,
        })
    }

    fn current_value(&self) -> Option<i32> {
        if self.finished || self.primary_pos >= self.primary.len() {
            None
        } else {
            Some(self.primary[self.primary_pos])
        }
    }

    fn advance(&mut self, buffer_pool: &mut BufferPool) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }

        self.primary_pos += 1;
        if self.primary_pos < self.primary.len() {
            return Ok(());
        }

        // Primary buffer exhausted; attempt to swap with the secondary or fetch new data.
        self.primary_pos = 0;

        if let Some(secondary) = self.secondary.take() {
            let mut old_primary = std::mem::replace(&mut self.primary, secondary);
            old_primary.clear();
            buffer_pool.release(old_primary);
            self.refill_count += 1;
            self.finished = self.primary.is_empty();
            return Ok(());
        }

        let mut buffer = buffer_pool.acquire();
        Self::fill_buffer(&mut self.reader, &mut buffer, self.buffer_capacity)?;

        if buffer.is_empty() {
            buffer_pool.release(buffer);
            self.finished = true;
            let mut old_primary = std::mem::take(&mut self.primary);
            old_primary.clear();
            buffer_pool.release(old_primary);
        } else {
            let mut old_primary = std::mem::replace(&mut self.primary, buffer);
            old_primary.clear();
            buffer_pool.release(old_primary);
            self.refill_count += 1;
        }

        Ok(())
    }

    fn try_prefetch(&mut self, mut buffer: Vec<i32>) -> io::Result<Option<Vec<i32>>> {
        if self.finished || self.secondary.is_some() {
            return Ok(Some(buffer));
        }

        Self::fill_buffer(&mut self.reader, &mut buffer, self.buffer_capacity)?;
        if buffer.is_empty() {
            Ok(Some(buffer))
        } else {
            self.secondary = Some(buffer);
            Ok(None)
        }
    }

    fn has_secondary_buffer(&self) -> bool {
        self.secondary.is_some()
    }

    fn refill_count(&self) -> u64 {
        self.refill_count
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn release_all_buffers(&mut self, buffer_pool: &mut BufferPool) {
        let primary = std::mem::take(&mut self.primary);
        if !primary.is_empty() || primary.capacity() > 0 {
            buffer_pool.release(primary);
        }
        if let Some(buffer) = self.secondary.take() {
            buffer_pool.release(buffer);
        }
    }

    fn take_primary_buffer(&mut self) -> Vec<i32> {
        std::mem::take(&mut self.primary)
    }

    fn fill_buffer(
        reader: &mut InputElementReader,
        buffer: &mut Vec<i32>,
        capacity: usize,
    ) -> io::Result<()> {
        buffer.clear();
        for _ in 0..capacity {
            match reader.next_element()? {
                Some(value) => buffer.push(value),
                None => break,
            }
        }
        Ok(())
    }
}

/// Implements a loser tree for selecting the next element among k runs.
struct LoserTree {
    losers: Vec<usize>,
}

impl LoserTree {
    fn new(k: usize) -> Self {
        Self { losers: vec![0; k] }
    }

    fn build(&mut self, runs: &[RunBuffer]) {
        for i in 0..runs.len() {
            self.losers[i] = 0;
        }
        for i in 0..runs.len() {
            self.replay(i, runs);
        }
    }

    fn winner(&self) -> usize {
        self.losers[0]
    }

    fn replay(&mut self, mut idx: usize, runs: &[RunBuffer]) {
        let mut parent = (self.losers.len() + idx) / 2;
        while parent > 0 {
            let loser_idx = self.losers[parent];
            if Self::compare(idx, loser_idx, runs) == Ordering::Greater {
                self.losers[parent] = idx;
                idx = loser_idx;
            }
            parent /= 2;
        }
        self.losers[0] = idx;
    }

    fn compare(a: usize, b: usize, runs: &[RunBuffer]) -> Ordering {
        let key_a = runs[a].current_value().unwrap_or(i32::MAX);
        let key_b = runs[b].current_value().unwrap_or(i32::MAX);
        key_b.cmp(&key_a)
    }
}

/// Byte-wise reader that yields integers from a file without loading all data into memory.
struct InputElementReader {
    bytes: io::Bytes<std::io::BufReader<File>>,
}

impl InputElementReader {
    fn new(file: File) -> io::Result<Self> {
        let reader = std::io::BufReader::with_capacity(8 * 1024, file);
        Ok(Self {
            bytes: reader.bytes(),
        })
    }

    fn next_element(&mut self) -> io::Result<Option<i32>> {
        let mut buf = Vec::new();

        loop {
            match self.bytes.next() {
                Some(Ok(b)) if b.is_ascii_whitespace() => {
                    if !buf.is_empty() {
                        break;
                    }
                }
                Some(Ok(b)) => {
                    buf.push(b);
                }
                Some(Err(e)) => return Err(e),
                None => {
                    if buf.is_empty() {
                        return Ok(None);
                    } else {
                        break;
                    }
                }
            }
        }

        let str_value = std::str::from_utf8(&buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let value = str_value
            .parse::<i32>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(Some(value))
    }
}

/// Statistics describing a single run file, used for reporting.
#[derive(Debug)]
pub struct RunStatistic {
    pub run_id: usize,
    pub length: u64,
}

/// Aggregated statistics about generated runs.
#[derive(Debug, Default)]
pub struct RunStatisticsSummary {
    pub entries: Vec<RunStatistic>,
}

impl RunStatisticsSummary {
    pub fn from_runs(runs: &[PathBuf]) -> io::Result<Self> {
        let mut entries = Vec::with_capacity(runs.len());
        for (idx, run_path) in runs.iter().enumerate() {
            let file = File::open(run_path)?;
            let mut reader = InputElementReader::new(file)?;
            let mut count = 0_u64;
            while reader.next_element()?.is_some() {
                count += 1;
            }
            entries.push(RunStatistic {
                run_id: idx,
                length: count,
            });
        }
        Ok(Self { entries })
    }

    pub fn summary(&self) -> Option<(usize, u64, u64, u64, f64)> {
        if self.entries.is_empty() {
            return None;
        }

        let mut total = 0_u64;
        let mut min_len = self.entries[0].length;
        let mut max_len = self.entries[0].length;
        for entry in &self.entries {
            total += entry.length;
            min_len = min_len.min(entry.length);
            max_len = max_len.max(entry.length);
        }

        Some((
            self.entries.len(),
            total,
            min_len,
            max_len,
            total as f64 / self.entries.len() as f64,
        ))
    }

    pub fn write_report<P: AsRef<Path>>(&self, output_path: P) -> io::Result<()> {
        let mut file = File::create(output_path)?;
        writeln!(file, "run_id,length")?;
        for entry in &self.entries {
            writeln!(file, "{},{}", entry.run_id, entry.length)?;
        }
        Ok(())
    }
}

