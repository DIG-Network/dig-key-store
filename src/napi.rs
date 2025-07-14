#[cfg(feature = "napi-bindings")]
mod napi_impl {
    use napi::bindgen_prelude::*;
    use napi_derive::napi;
    use std::time::Duration;
    use std::sync::Arc;
    use tokio::runtime::Runtime;

    use crate::{Cache, CacheOptions, CacheError};

    // Thread-local runtime for executing async code
    thread_local! {
        static RUNTIME: Runtime = Runtime::new().expect("Failed to create Tokio runtime");
    }

    // Helper function to convert CacheError to napi::Error
    fn convert_error(err: CacheError) -> napi::Error {
        napi::Error::new(Status::GenericFailure, format!("{}", err))
    }

    #[napi(object)]
    pub struct JsCacheOptions {
        pub max_memory_mb: u32,
        pub db_path: String,
    }

    #[napi]
    pub struct JsCache {
        cache: Arc<Cache>,
    }

    #[napi]
    impl JsCache {
        #[napi(constructor)]
        pub fn new(options: JsCacheOptions) -> napi::Result<Self> {
            let rust_options = CacheOptions {
                max_memory_mb: options.max_memory_mb as usize,
                db_path: options.db_path,
            };

            let cache = RUNTIME.with(|rt| {
                rt.block_on(async {
                    Cache::new(rust_options).await.map_err(convert_error)
                })
            })?;

            Ok(Self { cache: Arc::new(cache) })
        }

        #[napi]
        pub fn set(&self, key: String, value: Buffer, ttl_ms: Option<u32>) -> napi::Result<()> {
            let ttl = ttl_ms.map(|ms| Duration::from_millis(ms as u64));
            let cache = self.cache.clone();
            let value_vec = value.to_vec();

            RUNTIME.with(|rt| {
                rt.block_on(async {
                    cache.set(&key, &value_vec, ttl).await.map_err(convert_error)
                })
            })
        }

        #[napi]
        pub fn get(&self, key: String) -> napi::Result<Option<Buffer>> {
            let cache = self.cache.clone();

            RUNTIME.with(|rt| {
                rt.block_on(async {
                    let result = cache.get(&key).await.map_err(convert_error)?;
                    match result {
                        Some(value) => Ok(Some(Buffer::from(value))),
                        None => Ok(None),
                    }
                })
            })
        }

        #[napi]
        pub fn delete(&self, key: String) -> napi::Result<()> {
            let cache = self.cache.clone();

            RUNTIME.with(|rt| {
                rt.block_on(async {
                    cache.delete(&key).await.map_err(convert_error)
                })
            })
        }
    }
}
