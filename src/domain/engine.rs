use std::time::Duration;

use num_bigint::BigUint;
use num_traits::{One, Zero};

use crate::domain::calculation::{
    Calculation, CalculationResult, FibonacciInput, MatrixMultiplyInput, PrimeFactorsInput,
    SleepInput,
};
use crate::error::{AppError, AppResult};

const MAX_FIB_N: u64 = 200_000;
const MAX_PRIME_N: u64 = 1 << 62;
const MAX_MATRIX_DIM: usize = 256;
const MAX_SLEEP_MS: u64 = 60_000;

pub async fn execute(calculation: Calculation) -> AppResult<CalculationResult> {
    match calculation {
        Calculation::Fibonacci(input) => run_blocking(move || fibonacci(input)).await,
        Calculation::PrimeFactors(input) => run_blocking(move || prime_factors(input)).await,
        Calculation::MatrixMultiply(input) => run_blocking(move || matrix_multiply(input)).await,
        Calculation::Sleep(input) => sleep(input).await,
    }
}

async fn run_blocking<F>(f: F) -> AppResult<CalculationResult>
where
    F: FnOnce() -> AppResult<CalculationResult> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| AppError::Internal(format!("worker panicked: {e}")))?
}

fn fibonacci(input: FibonacciInput) -> AppResult<CalculationResult> {
    if input.n > MAX_FIB_N {
        return Err(AppError::BadRequest(format!(
            "fibonacci n must be <= {MAX_FIB_N}"
        )));
    }
    let mut a = BigUint::zero();
    let mut b = BigUint::one();
    for _ in 0..input.n {
        let next = &a + &b;
        a = b;
        b = next;
    }
    Ok(CalculationResult::Fibonacci {
        number: a.to_str_radix(10),
    })
}

fn prime_factors(input: PrimeFactorsInput) -> AppResult<CalculationResult> {
    if input.n < 2 {
        return Err(AppError::BadRequest("n must be >= 2".into()));
    }
    if input.n > MAX_PRIME_N {
        return Err(AppError::BadRequest(format!(
            "prime_factors n must be <= {MAX_PRIME_N}"
        )));
    }
    let mut n = input.n;
    let mut factors = Vec::new();
    let mut d: u64 = 2;
    while d.saturating_mul(d) <= n {
        while n % d == 0 {
            factors.push(d);
            n /= d;
        }
        d += if d == 2 { 1 } else { 2 };
    }
    if n > 1 {
        factors.push(n);
    }
    Ok(CalculationResult::PrimeFactors { factors })
}

fn matrix_multiply(input: MatrixMultiplyInput) -> AppResult<CalculationResult> {
    let MatrixMultiplyInput { a, b } = input;
    let rows_a = a.len();
    let cols_a = a.first().map(Vec::len).unwrap_or(0);
    let rows_b = b.len();
    let cols_b = b.first().map(Vec::len).unwrap_or(0);

    if rows_a == 0 || cols_a == 0 || rows_b == 0 || cols_b == 0 {
        return Err(AppError::BadRequest("matrices must be non-empty".into()));
    }
    if rows_a > MAX_MATRIX_DIM
        || cols_a > MAX_MATRIX_DIM
        || rows_b > MAX_MATRIX_DIM
        || cols_b > MAX_MATRIX_DIM
    {
        return Err(AppError::BadRequest(format!(
            "matrix dimensions must be <= {MAX_MATRIX_DIM}"
        )));
    }
    if cols_a != rows_b {
        return Err(AppError::BadRequest(format!(
            "cols(a)={cols_a} must equal rows(b)={rows_b}"
        )));
    }
    if a.iter().any(|row| row.len() != cols_a) || b.iter().any(|row| row.len() != cols_b) {
        return Err(AppError::BadRequest("matrices must be rectangular".into()));
    }

    let mut c = vec![vec![0.0_f64; cols_b]; rows_a];
    for i in 0..rows_a {
        for k in 0..cols_a {
            let aik = a[i][k];
            for j in 0..cols_b {
                c[i][j] += aik * b[k][j];
            }
        }
    }
    Ok(CalculationResult::MatrixMultiply { matrix: c })
}

async fn sleep(input: SleepInput) -> AppResult<CalculationResult> {
    if input.ms > MAX_SLEEP_MS {
        return Err(AppError::BadRequest(format!(
            "sleep ms must be <= {MAX_SLEEP_MS}"
        )));
    }
    tokio::time::sleep(Duration::from_millis(input.ms)).await;
    Ok(CalculationResult::Sleep { slept_ms: input.ms })
}
