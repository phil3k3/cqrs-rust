use prost::DecodeError;
use prost::EncodeError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Generic {0}")]
    Generic(String),

    #[error(transparent)]
    Tokio(#[from] tokio::sync::oneshot::error::RecvError),

    #[error(transparent)]
    DecodeError(#[from] DecodeError),

    #[error(transparent)]
    EncodeError(#[from] EncodeError),

    #[error(transparent)]
    SerdeError(#[from] serde_json::error::Error),
}
