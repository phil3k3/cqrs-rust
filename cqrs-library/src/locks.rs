use std::future::Future;
use std::sync::{Arc, Mutex};

pub type StdThreadSafeData<D> = Arc<Mutex<Option<D>>>;
pub struct StdThreadSafeDataManager<D> {
    data: StdThreadSafeData<D>
}

pub type TokioThreadSafeData<D> = Arc<tokio::sync::Mutex<Option<D>>>;

pub type TokioArcMutexData<D> = Arc<tokio::sync::Mutex<D>>;

impl<D> StdThreadSafeDataManager<D> {

    pub fn new(data: D) -> Self {
        return Self {
            data: Arc::new(Mutex::new(Some(data))),
        }
    }

    pub fn safe_call<F>(&mut self, func: F) where F: Fn(D) {
        let data_cloned = self.data.clone();
        let mut guard = data_cloned.lock().unwrap();

        if let Some(result) = guard.take() {
            func(result)
        }
    }
}

pub struct TokioThreadSafeDataManager<D> {
    data: TokioThreadSafeData<D>
}

pub struct TokioArcMutexDataManager<D> {
    data: TokioArcMutexData<D>
}

impl<D> TokioArcMutexDataManager<D> {
    pub(crate) fn new (data: Arc<tokio::sync::Mutex<D>>) -> Self {
        return Self {
            data
        }
    }

    pub fn wrapped(data: D) -> Self {
        return Self {
            data: Arc::new(tokio::sync::Mutex::new(data))
        }
    }

    pub async fn safe_call<F>(&self, func: F) where F: FnOnce(&mut D) {
        let mut guard = self.data.lock().await;
        func(&mut guard)
    }
}

impl<D> TokioThreadSafeDataManager<D> {
    pub fn new(data: Arc<tokio::sync::Mutex<Option<D>>>) -> Self {
        return Self {
            data
        }
    }

    pub fn wrapped(data: D) -> Self {
        return Self {
            data: Arc::new(tokio::sync::Mutex::new(Option::from(data)))
        }
    }

    pub async fn safe_call<F>(&mut self, func: F) where F: FnOnce(D) {
        let mut guard = self.data.lock().await;

        if let Some(result) = guard.take() {
            func(result)
        }
    }
    pub async fn safe_call_multiple_async<F, Fut>(&mut self, func: F) where F: Fn(D) -> Fut, Fut: Future<Output = ()> {
        let data_cloned = self.data.clone();
        let mut guard = data_cloned.lock().await;

        if let Some(result) = guard.take() {
            func(result).await
        }
    }
}

impl<D> Clone for StdThreadSafeDataManager<D> {
    fn clone(&self) -> Self {
        return StdThreadSafeDataManager {
            data: self.data.clone(),
        }
    }
}

impl<D> Clone for TokioThreadSafeDataManager<D> {
    fn clone(&self) -> Self {
        return TokioThreadSafeDataManager {
            data: self.data.clone()
        }
    }
}

impl<D> Clone for TokioArcMutexDataManager<D> {
    fn clone(&self) -> Self {
        return TokioArcMutexDataManager {
            data: self.data.clone()
        }
    }
}
