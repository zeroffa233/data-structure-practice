mod warmup_project;
use warmup_project::*;

fn main() {
    /*
    let matrix_a = Matrix::from_file(0, "./data/matrix_a.txt");
    let matrix_b = Matrix::from_file(1, "./data/matrix_b.txt");
    let cache = Cache::new(4, 4);
    let mut calculator = Calculator::new(matrix_a, matrix_b, cache, "./data/matrix_c.txt");
    calculator.calculate(Sequence::Sijk);
    */
    let dimensions = vec![3, 6, 10, 20, 50, 100];
    let cache_line_sizes = vec![1, 2, 4, 8, 16, 32, 64];
    let cache_line_numbers = vec![1, 2, 4, 8, 16, 32, 64];
    let sequences = vec![
        Sequence::Sijk,
        Sequence::Sikj,
        Sequence::Sjik,
        Sequence::Sjki,
        Sequence::Skij,
        Sequence::Skji,
    ];
    Evaluator::evaluate(dimensions, cache_line_sizes, cache_line_numbers, sequences);
}
