use prost::DecodeError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Generic {0}")]
    Generic(String),

    #[error(transparent)]
    Tokio(#[from] tokio::sync::oneshot::error::RecvError),

    #[error(transparent)]
    SerdeError(#[from] DecodeError),

    #[error(transparent)]
    SerdeError2(#[from] serde_json::error::Error),

    #[error(transparent)]
    SerdeError3(#[from] prost::EncodeError),
}
