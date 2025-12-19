use std::num::NonZeroUsize;
use std::result::Result;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lru::LruCache;
use sqlx::{Pool, Sqlite, SqlitePool};
use thiserror::Error;

mod database;
#[cfg(feature = "napi-bindings")]
pub mod napi;
static MAX_LRU_CACHE_ITEMS: usize = 100_000_000;

/// Checks if the given SQLx error is an SQLite "busy" or "locked" error
pub fn is_sqlite_busy_error(err: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = err {
        // SQLite error codes: SQLITE_BUSY = 5, SQLITE_LOCKED = 6
        if let Some(code) = db_err.code() {
            let code_str = code.to_string();
            return code_str == "5" || code_str == "6";
        }
    }
    false
}

/// Retries a database operation until it succeeds or encounters a non-busy error. Ideally this
/// should be called in a non-blocking task
pub async fn retry_on_busy<F, Fut, T>(operation: F) -> Result<T, CacheError>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<T, sqlx::Error>> + Send,
    T: Send,
{
    // Use a constant, minimal delay for high performance
    let retry_delay_ms = 5; // Fixed 5ms delay between retries
    let max_retries = 10;
    let mut retry_count = 0;

    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(err) if is_sqlite_busy_error(&err) => {
                retry_count += 1;
                if retry_count > max_retries {
                    println!(
                        "Database is busy/locked, max retries ({}) exceeded",
                        max_retries
                    );
                    return Err(CacheError::DatabaseError(err));
                }

                // If we get a busy error, wait with a constant minimal delay and retry
                println!(
                    "Database is busy/locked, retrying in {}ms (attempt {}/{})",
                    retry_delay_ms, retry_count, max_retries
                );
                tokio::time::sleep(std::time::Duration::from_millis(retry_delay_ms)).await;
                continue;
            }
            Err(err) => return Err(CacheError::DatabaseError(err)),
        }
    }
}

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("Database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    #[error("Key not found")]
    KeyNotFound,

    #[error("Value expired")]
    ValueExpired,

    #[error("Migration error: {0}")]
    MigrationError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Pool error: {0}")]
    PoolError(String),

    #[error("DB connection error: {0}")]
    DbConnectionError(#[from] database::DbError),

    #[error("System time error: {0}")]
    SystemTimeError(#[from] std::time::SystemTimeError),

    #[error("Mutex lock error")]
    MutexLockError,

    #[error("Invalid value: {0}")]
    InvalidValue(String),

    #[error(
        "TTL value overflow. Current time (seconds) + TTL (seconds) must be less than (2^63)-1 seconds"
    )]
    TtlValueOverflow,
}

#[derive(Debug, Clone)]
pub struct CacheOptions {
    pub max_memory_mb: usize,
    pub db_path: String,
}

/// A key-value cache with both in-memory and persistent storage.
///
/// The cache stores values in both memory (using an LRU cache) and in a SQLite database.
/// Values can be set with an optional time-to-live (TTL).
///
/// # Examples
///
/// ```
/// use dig_key_store::{Cache, CacheOptions};
/// use std::time::Duration;
///
/// #[tokio::main]
/// async fn main() {
///     // Create a cache with default options
///     let options = CacheOptions {max_memory_mb: 100, db_path: "tests/db/cargo_unit_tests.sqlite".to_string()};
///     let cache = Cache::new(options).await.expect("Failed to create cache");
///
///     // Set a value
///     let key = "example_key";
///     let value = b"example_value";
///     cache.set(key, value, None).await.expect("Failed to set value");
///
///     // Get a value
///     let result = cache.get(key).await.expect("Failed to get value");
///     assert_eq!(result, Some(value.to_vec()));
/// }
/// ```
pub struct Cache {
    memory_cache: Arc<Mutex<LruCache<String, CacheData>>>,
    db_pool: Pool<Sqlite>,
    options: CacheOptions,
    current_memory_usage: Arc<Mutex<usize>>,
}

impl Cache {
    pub async fn new(options: CacheOptions) -> Result<Self, CacheError> {
        // Set up database connection using the db_connection module
        let db_pool = database::init(&options.db_path).await?;

        // Create LRU cache
        let max_items = NonZeroUsize::new(MAX_LRU_CACHE_ITEMS).ok_or_else(|| {
            CacheError::InvalidValue("MAX_LRU_CACHE_ITEMS cannot be zero".to_string())
        })?;
        let memory_cache = Arc::new(Mutex::new(LruCache::new(max_items)));

        // Initialize memory usage tracker
        let current_memory_usage = Arc::new(Mutex::new(0));

        let cache = Self {
            memory_cache,
            db_pool,
            options,
            current_memory_usage,
        };

        Ok(cache)
    }

    pub async fn set(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<(), CacheError> {
        let expires = Self::calculate_expiry_secs(ttl)?;

        // Update SQLite
        self.set_in_db(key, value, expires).await?;

        // Update memory cache and track memory usage
        let mut memory_cache = self
            .memory_cache
            .lock()
            .map_err(|_| CacheError::MutexLockError)?;

        // Add new value size to memory usage
        let new_value = value.to_vec();
        let new_size = new_value.len() + key.len();

        // Update memory cache
        let maybe_old_value = memory_cache.put(
            key.to_string(),
            CacheData {
                data: new_value,
                expires,
            },
        );

        // Check if key already exists in memory cache
        let removed_size = if let Some(old_value) = maybe_old_value {
            old_value.data.len() + key.len()
        } else {
            0
        };

        self.update_memory_usage(Some(new_size), Some(removed_size))?;

        Ok(())
    }

    /// Retrieves a value from the cache. Values cached in memory are immediately returned and lazily
    /// reconciled with the DB. If there is a memory cache miss, or the value in memory is expired,
    /// the value will be retrieved from the database and added to the memory cache. If the value is expired
    /// in the DB or missing from the DB it will be lazily removed.
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        // Check if key exists in memory cache
        let maybe_memory_value = {
            let mut memory_cache = self
                .memory_cache
                .lock()
                .map_err(|_| CacheError::MutexLockError)?;
            if let Some(value) = memory_cache.get(key) {
                // spawn non-blocking task to check that the value is valid in the database
                let memory_cache_arc = self.memory_cache.clone();
                let db_pool = self.db_pool.clone();
                let key_arc = Arc::new(key.to_string());

                // non-blocking task to check that the value is valid and present in the database
                tokio::spawn(async move {
                    // without an established logger, dont bother handling errors in this fn, just give up
                    let maybe_db_value =
                        match Self::get_from_db_impl(&db_pool, key_arc.as_str()).await {
                            Ok(db_value) => db_value,
                            Err(_) => return, // if we cant get the value from the db, nothing to do
                        };

                    if let Some(db_value) = maybe_db_value {
                        if db_value.expired() {
                            // Remove expired entry from DB and cache - another thread may have updated the TTL
                            Self::delete_from_db_impl(&db_pool, key_arc.as_str())
                                .await
                                .ok();
                            let mut memory_cache = match memory_cache_arc.lock() {
                                Ok(cache) => cache,
                                Err(_) => return, // if we cant lock the memory cache, nothing to do
                            };

                            memory_cache.pop(key_arc.as_str());
                        } else {
                            Self::update_last_accessed(&db_pool, key_arc.as_str())
                                .await
                                .ok();
                        }
                    } else {
                        let mut memory_cache = match memory_cache_arc.lock() {
                            Ok(cache) => cache,
                            Err(_) => return, // if we cant lock the memory cache, nothing to do
                        };

                        memory_cache.pop(key_arc.as_str());
                    }
                });

                if value.expired() {
                    // Remove expired entry from memory cache
                    memory_cache.pop(key);
                    None
                } else {
                    Some(value.data.clone())
                }
            } else {
                None
            }
        };

        if maybe_memory_value.is_none() {
            let maybe_db_value = self.get_from_db(key).await?;
            if let Some(db_value) = maybe_db_value {
                if db_value.expired() {
                    // no memory value and expired db value -> Remove expired entry from DB
                    self.delete_from_db(key).await?;
                    Ok(None)
                } else {
                    // valid db value -> add to memory cache and track memory usage
                    let mut memory_cache = self
                        .memory_cache
                        .lock()
                        .map_err(|_| CacheError::MutexLockError)?;

                    let size = db_value.data.len() + key.len();
                    self.update_memory_usage(Some(size), None)?;

                    memory_cache.put(key.to_string(), db_value.clone());

                    Ok(Some(db_value.data))
                }
            } else {
                // no value in the memory cache and no value in the database
                Ok(None)
            }
        } else {
            Ok(maybe_memory_value)
        }
    }

    /// Deletes a value from both the memory cache and the database.
    pub async fn delete(&self, key: &str) -> Result<(), CacheError> {
        // Remove from memory cache and update memory usage
        {
            let mut memory_cache = self
                .memory_cache
                .lock()
                .map_err(|_| CacheError::MutexLockError)?;

            // Get the value being deleted to calculate its size
            if let Some(value) = memory_cache.pop(key) {
                // Subtract the size of the key and value from memory usage
                let size = value.data.len() + key.len();
                self.update_memory_usage(None, Some(size))?
            }
        }

        // Remove from database
        self.delete_from_db(key).await?;

        Ok(())
    }

    // Private helper methods

    async fn set_in_db(
        &self,
        key: &str,
        value: &[u8],
        expires: Option<i64>,
    ) -> Result<(), CacheError> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

        // Use the retry function to handle busy errors
        let db_pool = self.db_pool.clone();
        retry_on_busy(move || {
            let key = key.to_string();
            let value = value.to_vec();
            let db_pool = db_pool.clone();
            async move {
                let existing = database::key_exists(&db_pool, &key).await?;

                if existing {
                    database::update_cache_entry(&db_pool, &key, &value, expires, now).await?;
                } else {
                    database::insert_cache_entry(&db_pool, &key, &value, expires, now).await?;
                }

                Ok(())
            }
        })
        .await?;

        Ok(())
    }

    /// Retrieves a value from the database. If the value is expired, it will be removed from the database.
    async fn get_from_db(&self, key: &str) -> Result<Option<CacheData>, CacheError> {
        Self::get_from_db_impl(&self.db_pool, key).await
    }

    /// Retrieves a value from the database. If the value is expired, it will be removed from the database.
    async fn get_from_db_impl(
        db_pool: &Pool<Sqlite>,
        key: &str,
    ) -> Result<Option<CacheData>, CacheError> {
        let key_str = key.to_string();

        let result = retry_on_busy(move || {
            let key = key_str.clone();
            let db_pool = db_pool.clone();
            async move { database::get_cache_entry(&db_pool, &key).await }
        })
        .await?;

        match result {
            Some(entry) => Ok(Some(CacheData {
                data: entry.cache_value,
                expires: entry.expires,
            })),
            None => Ok(None),
        }
    }

    async fn delete_from_db(&self, key: &str) -> Result<(), CacheError> {
        Self::delete_from_db_impl(&self.db_pool, key).await
    }

    async fn delete_from_db_impl(db_pool: &Pool<Sqlite>, key: &str) -> Result<(), CacheError> {
        retry_on_busy(move || {
            let key = key.to_string();
            let db_pool = db_pool.clone();
            async move { database::delete_cache_entry(&db_pool, &key).await }
        })
        .await?;

        Ok(())
    }

    /// Updates the last accessed time for the specified key. This should be called in a non-blocking task.
    async fn update_last_accessed(db_pool: &Pool<Sqlite>, key: &str) -> Result<(), CacheError> {
        println!("Updating last accessed time for key: {}", key);
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

        retry_on_busy(move || {
            let key = key.to_string();
            let db_pool_local = db_pool.clone();
            async move { database::update_last_accessed(&db_pool_local, &key, now).await }
        })
        .await?;

        Ok(())
    }

    /// Calculate the time of expiration in seconds
    #[inline]
    fn calculate_expiry_secs(ttl: Option<Duration>) -> Result<Option<i64>, CacheError> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        ttl.map(|duration| -> Result<i64, CacheError> {
            let expiration_u64 = now + duration.as_secs();
            let expiration_i64 =
                i64::try_from(expiration_u64).map_err(|_| CacheError::TtlValueOverflow)?;
            Ok(expiration_i64)
        })
        .transpose()
    }

    fn update_memory_usage(
        &self,
        add: Option<usize>,
        sub: Option<usize>,
    ) -> Result<(), CacheError> {
        {
            let mut memory_usage = self
                .current_memory_usage
                .lock()
                .map_err(|_| CacheError::MutexLockError)?;
            if let Some(add) = add {
                *memory_usage = memory_usage.saturating_add(add);
            }
            if let Some(sub) = sub {
                *memory_usage = memory_usage.saturating_sub(sub);
            }
        }

        self.evict_if_memory_pressure();
        Ok(())
    }

    /// NON-BLOCKING; Lazily evicts items from the LRU cache if memory usage exceeds the limit
    fn evict_if_memory_pressure(&self) {
        let memory_cache_arc = self.memory_cache.clone();
        let memory_usage_arc = self.current_memory_usage.clone();
        let max_memory_mb = self.options.max_memory_mb;

        tokio::spawn(async move {
            // return on locking error because this is a non-blocking task so the
            let mut memory_cache = match memory_cache_arc.lock() {
                Ok(cache) => cache,
                Err(_) => return,
            };
            let mut memory_usage = match memory_usage_arc.lock() {
                Ok(usage) => usage,
                Err(_) => return,
            };

            let max_memory_bytes = max_memory_mb * 1024 * 1024;
            while *memory_usage > max_memory_bytes && memory_cache.len() > 1 {
                // Evict the least recently used item
                if let Some((evicted_key, evicted_value)) = memory_cache.pop_lru() {
                    // Subtract the size of the evicted key and value from memory usage
                    let evicted_size = evicted_key.len() + evicted_value.data.len();
                    *memory_usage = memory_usage.saturating_sub(evicted_size);
                    println!(
                        "Evicted key '{}' due to memory pressure. Memory usage: {}/{} bytes",
                        evicted_key, *memory_usage, max_memory_bytes
                    );
                } else {
                    // No more items to evict
                    break;
                }
            }
        });
    }

    /// Cleans up expired keys from the database
    ///
    /// This method can be called manually to remove expired keys from both the memory cache and the database.
    /// It returns the number of keys that were removed.
    pub async fn cleanup_expired_keys(&self) -> Result<usize, CacheError> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

        let db_pool = self.db_pool.clone();

        // Get all expired keys
        let expired_keys = retry_on_busy(move || {
            let db_pool = db_pool.clone();
            let now = now;
            async move { database::get_expired_keys(&db_pool, now).await }
        })
        .await?;

        if expired_keys.is_empty() {
            println!("No expired keys to clean up");
            return Ok(0);
        }

        println!("Found {} expired keys to clean up", expired_keys.len());

        // Extract key strings from CacheKey structs
        let key_strings: Vec<String> = expired_keys.into_iter().map(|k| k.cache_key).collect();

        // Remove expired keys from memory cache
        // code block to hastily drop the mutex guard and release the lock - deadlock avoidance
        {
            let mut memory_cache = self
                .memory_cache
                .lock()
                .map_err(|_| CacheError::MutexLockError)?;

            for key in &key_strings {
                if let Some(value) = memory_cache.pop(key) {
                    // Subtract the size of the key and value from memory usage
                    let size = value.data.len() + key.len();
                    self.update_memory_usage(None, Some(size));
                }
            }
        }

        // Delete expired keys from database in batch
        let db_pool = self.db_pool.clone();
        let count = key_strings.len();

        retry_on_busy(move || {
            let db_pool = db_pool.clone();
            let key_strings = key_strings.clone();
            async move { database::delete_keys_batch(&db_pool, &key_strings).await }
        })
        .await?;

        println!("Cleaned up {} expired keys", count);

        Ok(count)
    }
}

/// Data structure representing a cache entry in the LRU cache. Time of expiration included.
/// The DB stores the time of expiration as a separate column. DB helpers build this type
/// when getting data
#[derive(Debug, Clone)]
struct CacheData {
    pub data: Vec<u8>,
    pub expires: Option<i64>,
}

impl CacheData {
    /// True if the specified key has expired based on the provided expiration time in SECONDS
    /// If the system time is before the unix epoch, it will return true (at that point all bets are
    /// off anyway)
    #[inline]
    fn expired(&self) -> bool {
        expired(self.expires)
    }
}

/// True if the specified key has expired based on the provided expiration time in SECONDS
/// If the system time is before the unix epoch, it will return true (at that point all bets are
/// off anyway)
#[inline]
fn expired(expiry_secs: Option<i64>) -> bool {
    if let Some(expiration) = expiry_secs {
        let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_secs() as i64,
            Err(_) => return true,
        };
        now > expiration
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_cleanup_expired_entries() {
        println!("UNIT TEST: Testing cleanup of expired entries");

        // Create a test cache
        let db_path = "tests/db/cargo_unit_tests.sqlite";
        clean_db_files_for_unit_test(db_path);

        println!("Creating test cache with database path: {}", db_path);
        // Create a cache with a very short cleanup interval
        let options = CacheOptions {
            max_memory_mb: 10,
            db_path: db_path.to_string(),
        };

        let cache = Cache::new(options)
            .await
            .expect("Failed to create cache for test");
        println!("Successfully created cache instance");

        let key = "lib_rs_test_cleanup_expired_entries";
        let value = b"test_value";

        // Set with 1 second TTL
        println!("Setting key '{}' with 1 second TTL", key);
        cache
            .set(key, value, Some(Duration::from_secs(1)))
            .await
            .expect("Failed to set key in test");

        // Should be available immediately
        println!("Verifying key is available immediately after setting");
        let result = cache.get(key).await.expect("Failed to get key in test");
        assert_eq!(result, Some(value.to_vec()));
        println!("Key was successfully retrieved immediately after setting");

        // Wait for expiration and cleanup
        println!("Waiting for 2 seconds to allow key to expire...");
        sleep(Duration::from_secs(2)).await;
        println!("Wait complete, key should now be expired");

        // Call get multiple times to ensure the expiration check is triggered
        // The first call might not trigger the check if the value is still in the memory cache
        println!("Attempting to retrieve expired key (may require multiple attempts)");
        for attempt in 1..=3 {
            println!("Attempt #{} to verify key has expired", attempt);
            let result = cache
                .get(key)
                .await
                .expect("Failed to get key in test attempt");
            if result.is_none() {
                // Test passes if we get None
                println!("SUCCESS: Key has expired and was properly removed from cache");
                return;
            }
            // Wait a bit before trying again
            println!("Key still exists in cache, waiting 500ms before next attempt");
            sleep(Duration::from_millis(500)).await;
        }

        // If we get here, the test fails
        panic!("Value did not expire after multiple attempts");
    }

    #[tokio::test]
    async fn test_manual_cleanup() {
        println!("UNIT TEST: Testing manual cleanup");

        let db_path = "tests/db/cargo_unit_tests_manual_cleanup.sqlite";
        clean_db_files_for_unit_test(db_path);

        println!("Creating test cache with database path: {}", db_path);
        // Create a cache
        let options = CacheOptions {
            max_memory_mb: 10,
            db_path: db_path.to_string(),
        };

        // Create a new cache instance
        let cache = Cache::new(options)
            .await
            .expect("Failed to create cache for test");
        println!("Successfully created cache instance");

        // Add several keys with short TTLs
        for i in 0..5 {
            let key = format!("manual_cleanup_test_key_{}", i);
            let value = format!("value_{}", i).into_bytes();

            // Set with 2 second TTL
            println!("Setting key '{}' with 2 second TTL", key);
            cache
                .set(&key, &value, Some(Duration::from_secs(2)))
                .await
                .unwrap();
        }

        // Wait for keys to expire (3 seconds should be enough)
        println!("Waiting for 3 seconds to allow keys to expire...");
        sleep(Duration::from_secs(3)).await;
        println!("Wait complete, keys should now be expired");

        // Manually run the cleanup
        println!("Running manual cleanup");
        let cleaned_count = cache
            .cleanup_expired_keys()
            .await
            .expect("Failed to run cleanup");
        assert_eq!(cleaned_count, 5, "Should have cleaned up 5 keys");

        // Verify all keys have been removed
        for i in 0..5 {
            let key = format!("manual_cleanup_test_key_{}", i);
            let result = cache.get(&key).await.expect("Failed to get key in test");
            assert_eq!(
                result, None,
                "Key '{}' should have been removed by manual cleanup",
                key
            );
        }

        println!("SUCCESS: All keys were properly removed by manual cleanup");
    }

    fn clean_db_files_for_unit_test(db_path: &str) {
        let db_paths = vec![
            db_path.to_string(),
            format!("{}-wal", db_path.to_string()),
            format!("{}-shm", db_path.to_string()),
        ];
        // Remove the database file if it exists

        for db_path in db_paths {
            if std::path::Path::new(&db_path).exists() {
                std::fs::remove_file(&db_path).expect(
                    format!("Failed to remove existing database file: {}", db_path).as_str(),
                );
            }
        }

        println!("Removed existing database files");
    }
}
