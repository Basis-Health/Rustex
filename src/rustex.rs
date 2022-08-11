use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use pyo3::prelude::*;
use pyo3::exceptions::{PyRuntimeError};

use log::{debug, error};

struct RustexCore {
    map: RwLock<HashMap<String, String>>,
    contexts: RwLock<HashSet<String>>,
    sleep_duration: std::time::Duration,
    timeout_duration: std::time::Duration,
}

impl Default for RustexCore {
    fn default() -> Self {
        RustexCore {
            map: RwLock::new(HashMap::new()),
            contexts: RwLock::new(HashSet::new()),
            sleep_duration: std::time::Duration::from_millis(100),
            timeout_duration: std::time::Duration::from_secs(15),
        }
    }
}

#[pyclass]
#[derive(Default)]
pub struct Rustex(Arc<RustexCore>);

#[pymethods]
impl Rustex {
    /// Creates a new Rustex instance.
    #[new]
    fn new() -> Self {
        Self::default()
    }

    /// set_sleep_duration(duration: int, /)
    /// --
    ///
    /// Sets the default sleep duration in milliseconds.
    fn set_sleep_duration(&mut self, duration: u64) {
        (*Arc::get_mut(&mut self.0).unwrap()).sleep_duration = std::time::Duration::from_millis(duration);
    }

    /// set_timeout_duration(duration: int, /)
    /// --
    ///
    /// Sets the default timeout duration in seconds.
    fn set_timeout_duration(&mut self, duration: u64) {
        (*Arc::get_mut(&mut self.0).unwrap()).timeout_duration = std::time::Duration::from_secs(duration);
    }

    /// add_context(context: str, /)
    /// --
    ///
    /// Adds a context to the list of contexts. Note that for a mutex to be
    /// created, the context must be added before it is used.
    ///
    /// Note: This function will throw an exception if the context is already
    /// in the list of contexts.
    ///
    /// Note: This function is blocking.
    fn add_context(&self, context: String) -> PyResult<()> {
        debug!("Adding context {}", context);
        let value_didnt_exist = self.0.contexts.write().unwrap().insert(context);
        if value_didnt_exist { Ok(()) } else { Err(PyRuntimeError::new_err("Added context that already exists!")) }
    }

    /// remove_context(context: str, /)
    /// --
    ///
    /// Removes a context from the list of contexts.
    ///
    /// Note: This function will throw an exception if the context is not in the
    /// list of contexts.
    ///
    /// Note: This function is blocking.
    fn remove_context(&self, context: String) -> PyResult<()> {
        debug!("Removing context {}", context);
        let value_existed = self.0.contexts.write().unwrap().remove(&context);
        if value_existed { Ok(()) } else { Err(PyRuntimeError::new_err("Removing context that doesn't exist!")) }
    }

    /// acquire_mutex(mutex_name: str, context: str, /)
    /// --
    ///
    /// Returns a coroutine that acquires a mutex with the given name within the given context.
    /// If the mutex is already acquired, the coroutine will block until the mutex is released.
    ///
    /// Note: This function will throw an exception if the context is not in the
    /// list of contexts.
    ///
    /// Note: This function will timeout if the mutex is not acquired within the
    /// timeout duration. If the timeout is exceeded, the coroutine will be
    /// cancelled with a RuntimeError.
    fn acquire_mutex<'a>(&self, py: Python<'a>, mutex_name: String, context: String) -> PyResult<&'a PyAny> {
        let core = Arc::clone(&self.0);

        pyo3_asyncio::async_std::future_into_py(py, async move {
            if !(core.contexts.read().unwrap().contains(&context)) {
                error!("Failed to acquire mutex {} as context {} is not registered", mutex_name, context);
                return Err(PyRuntimeError::new_err("Trying to acquire mutex for context that is not registered, did you forgot an await?"))
            }

            let mut success = false;
            let start_time = std::time::Instant::now();

            while !success {
                let mut map = core.map.write().unwrap();
                if let Some(kp) = map.get(&mutex_name) {
                    if !core.contexts.read().unwrap().contains(kp) {
                        debug!("Mutex {} is locked by context {}, but that context doesn't exist anymore, will free mutex", mutex_name, kp);
                        map.remove(&mutex_name);
                    } else {
                        debug!("Mutex {} is locked by context {}, waiting {}ms", mutex_name, kp, core.sleep_duration.as_millis());
                        if start_time.elapsed() > core.timeout_duration {
                            return Err(PyRuntimeError::new_err("Timeout while waiting for mutex"));
                        }
                        std::thread::sleep(core.sleep_duration);
                        continue;
                    }
                }

                map.insert(mutex_name.clone(), context.clone());
                success = true;
            }

            debug!("Mutex {} acquired by context {}", mutex_name, context);
            Ok(Python::with_gil(|py| py.None()))
        })
    }

    /// try_acquire_mutex(mutex_name: str, context: str, /)
    /// --
    ///
    /// Returns a coroutine that tries to acquire the mutex which will return true on success
    /// or false if the mutex is already being held.
    ///
    /// Note: This function will not try to continuously acquire the mutex
    fn try_acquire_mutex<'a>(&self, py: Python<'a>, mutex_name: String, context: String) -> PyResult<&'a PyAny> {
        let core = Arc::clone(&self.0);

        pyo3_asyncio::async_std::future_into_py(py, async move {
            if !(core.contexts.read().unwrap().contains(&context)) {
                error!("Failed to acquire mutex {} as context {} is not registered", mutex_name, context);
                return Err(PyRuntimeError::new_err("Trying to acquire mutex for context that is not registered, did you forgot an await?"))
            }

            let mut map = core.map.write().unwrap();
            if let Some(kp) = map.get(&mutex_name) {
                if !core.contexts.read().unwrap().contains(kp) {
                    debug!("Mutex {} is locked by context {}, but that context doesn't exist anymore, will free mutex", mutex_name, kp);
                    map.remove(&mutex_name);
                } else {
                    debug!("Mutex {} is locked by context {}", mutex_name, kp);
                    return Ok(false)
                }
            }

            debug!("Mutex {} acquired by context {}", mutex_name, context);
            map.insert(mutex_name.clone(), context.clone());
            Ok(true)
        })
    }

    /// release_mutex(mutex_name: str, context: str, /)
    /// --
    ///
    /// Returns a coroutine that releases a mutex with the given name within the given context.
    ///
    /// Note: This function will throw an exception if the context is not in the
    /// list of contexts.
    ///
    /// Note: This function will throw an exception if the mutex is not acquired by the context.
    ///
    /// Note: This function will throw an exception if the mutex is not available.
    ///
    /// Note: This function will not timeout.
    fn release_mutex<'a>(&self, py: Python<'a>, mutex_name: String, context: String) -> PyResult<&'a PyAny> {
        let core = Arc::clone(&self.0);

        pyo3_asyncio::async_std::future_into_py(py, async move {
            if !(core.contexts.read().unwrap().contains(&context)) {
                error!("Failed to release mutex {} as context {} is not registered", mutex_name, context);
                return Err(PyRuntimeError::new_err("Trying to release mutex for context that is not registered, did you forgot an await?"))
            }

            let mut map = core.map.write().unwrap();
            if let Some(kp) = map.get(&mutex_name) {
                if kp == &context {
                    map.remove(&mutex_name);
                    debug!("Mutex {} released by context {}", mutex_name, context);
                    Ok(())
                } else {
                    Err(PyRuntimeError::new_err("Trying to release mutex that is not locked by context"))
                }
            } else {
                Err(PyRuntimeError::new_err("Trying to release mutex that is not registered"))
            }
        })
    }

    /// is_context_holding_mutex(mutex_name: str, context: str, /)
    /// --
    ///
    /// Checks whether the provided context is holding the given mutex
    ///
    /// Note: This function will also return False, if the context or the mutex is not available
    fn is_context_holding_mutex(&self, mutex_name: String, context: String) -> PyResult<bool> {
        if let Some(x) = self.0.map.read().unwrap().get(&mutex_name) {
            Ok(x == &context)
        } else {
            Ok(false)
        }
    }
}