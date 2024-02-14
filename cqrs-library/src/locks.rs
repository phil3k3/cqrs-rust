use std::sync::{Arc, Mutex};

pub type ThreadSafeData<D> = Arc<Mutex<Option<D>>>;

pub struct ThreadSafeDataManager<D> {
    data: ThreadSafeData<D>
}

impl<D> ThreadSafeDataManager<D> {

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

impl<D> Clone for ThreadSafeDataManager<D> {
    fn clone(&self) -> Self {
        return ThreadSafeDataManager {
            data: self.data.clone(),
        }
    }
}