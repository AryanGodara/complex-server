use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("placeholder")]
    Placeholder,
}

pub type AppResult<T> = Result<T, AppError>;
