mod project_1;
use project_1::*;

fn main() {
    let source_generator = SourceFileGenerator::new(100, -1000, 1000, "nums.txt".to_string());
    source_generator.generate_file();

    let run_generator = RunGenerator::new(
        10,
        "nums.txt".to_string(),
        "merge_passes/merge_pass_0".to_string(),
    );
    run_generator.generate_run_file();

    let mut merger = Merger::new(
        "merge_passes/merge_pass_0".to_string(),
        "merge_passes".to_string(),
    );

    merger.merge();
}
