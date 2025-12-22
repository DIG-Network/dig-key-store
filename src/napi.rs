#[cfg(feature = "napi-bindings")]
mod napi_impl {
    use napi::bindgen_prelude::*;
    use napi_derive::napi;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::{Cache, CacheError, CacheOptions};

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
        #[napi(factory)]
        pub async fn create(options: JsCacheOptions) -> napi::Result<Self> {
            let rust_options = CacheOptions {
                max_memory_mb: options.max_memory_mb as usize,
                db_path: options.db_path,
            };

            let cache = Cache::new(rust_options).await.map_err(convert_error)?;

            Ok(Self {
                cache: Arc::new(cache),
            })
        }

        #[napi]
        pub async fn set(
            &self,
            key: String,
            value: Buffer,
            ttl_ms: Option<u32>,
        ) -> napi::Result<()> {
            let ttl = ttl_ms.map(|ms| Duration::from_millis(ms as u64));
            let cache = self.cache.clone();
            let value_vec = value.to_vec();

            cache
                .set(&key, &value_vec, ttl)
                .await
                .map_err(convert_error)
        }

        #[napi]
        pub async fn get(&self, key: String) -> napi::Result<Option<Buffer>> {
            let result = self.cache.get(&key).await.map_err(convert_error)?;
            match result {
                Some(value) => Ok(Some(Buffer::from(value))),
                None => Ok(None),
            }
        }

        #[napi]
        pub async fn delete(&self, key: String) -> napi::Result<()> {
            self.cache.delete(&key).await.map_err(convert_error)
        }
    }
}
