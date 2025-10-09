mod warmup_project;
use warmup_project::*;

fn main() {
    let matrix_a = Matrix::from_file(0, "./data/matrix_a.txt");
    let matrix_b = Matrix::from_file(1, "./data/matrix_b.txt");
    let cache = Cache::new(4, 4);
    let mut calculator = Calculator::new(matrix_a, matrix_b, cache, "./data/matrix_c.txt");
    calculator.calculate(Sequence::Sijk);
}
