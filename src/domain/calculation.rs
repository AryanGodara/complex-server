use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum Calculation {
    Fibonacci(FibonacciInput),
    PrimeFactors(PrimeFactorsInput),
    MatrixMultiply(MatrixMultiplyInput),
    Sleep(SleepInput),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FibonacciInput {
    pub n: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrimeFactorsInput {
    pub n: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatrixMultiplyInput {
    pub a: Vec<Vec<f64>>,
    pub b: Vec<Vec<f64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SleepInput {
    pub ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CalculationResult {
    Fibonacci { number: String },
    PrimeFactors { factors: Vec<u64> },
    MatrixMultiply { matrix: Vec<Vec<f64>> },
    Sleep { slept_ms: u64 },
}

impl Calculation {
    pub fn kind(&self) -> &'static str {
        match self {
            Calculation::Fibonacci(_) => "fibonacci",
            Calculation::PrimeFactors(_) => "prime_factors",
            Calculation::MatrixMultiply(_) => "matrix_multiply",
            Calculation::Sleep(_) => "sleep",
        }
    }
}
