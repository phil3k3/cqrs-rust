#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Generic {0}")]
    Generic(String),

    #[error(transparent)]
    Tokio(#[from] tokio::sync::oneshot::error::RecvError),
}
