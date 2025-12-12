use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::sync::{Arc, Mutex};
use std::num::NonZeroUsize;
use std::result::Result;

use sqlx::{Sqlite, Pool};
use lru::LruCache;
use thiserror::Error;

// Import database queries
use crate::database::{
    key_exists, 
    update_cache_entry, 
    insert_cache_entry, 
    get_cache_entry, 
    update_last_accessed, 
    delete_cache_entry, 
    get_expired_keys, 
    delete_keys_batch
};

mod database;
#[cfg(feature = "napi-bindings")]
pub mod napi;
static MAX_LRU_CACHE_ITEMS: usize = 1_000_000;

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

/// Retries a database operation until it succeeds or encounters a non-busy error
pub async fn retry_on_busy<F, Fut, T>(operation: F) -> Result<T, CacheError>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<T, sqlx::Error>> + Send,
    T: Send,
{
    // Use a constant, minimal delay for high performance
    let retry_delay_ms = 5; // Fixed 5ms delay between retries
    let max_retries = 1000;
    let mut retry_count = 0;

    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(err) if is_sqlite_busy_error(&err) => {
                retry_count += 1;
                if retry_count > max_retries {
                    println!("Database is busy/locked, max retries ({}) exceeded", max_retries);
                    return Err(CacheError::DatabaseError(err));
                }

                // If we get a busy error, wait with a constant minimal delay and retry
                println!("Database is busy/locked, retrying in {}ms (attempt {}/{})", 
                         retry_delay_ms, retry_count, max_retries);
                tokio::time::sleep(std::time::Duration::from_millis(retry_delay_ms)).await;
                continue;
            },
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
    memory_cache: Arc<Mutex<LruCache<String, Vec<u8>>>>,
    db_pool: Pool<Sqlite>,
    options: CacheOptions,
    current_memory_usage: Arc<Mutex<usize>>,
}

impl Cache {

    /// Evicts items from the LRU cache if memory usage exceeds the limit
    fn evict_if_memory_pressure(&self, memory_cache: &mut LruCache<String, Vec<u8>>, memory_usage: &mut usize) {
        let max_memory_bytes = self.options.max_memory_mb * 1024 * 1024;
        while *memory_usage > max_memory_bytes && memory_cache.len() > 1 {
            // Evict the least recently used item
            if let Some((evicted_key, evicted_value)) = memory_cache.pop_lru() {
                // Subtract the size of the evicted key and value from memory usage
                let evicted_size = evicted_key.len() + evicted_value.len();
                *memory_usage = memory_usage.saturating_sub(evicted_size);
                println!("Evicted key '{}' due to memory pressure. Memory usage: {}/{} bytes", 
                         evicted_key, *memory_usage, max_memory_bytes);
            } else {
                // No more items to evict
                break;
            }
        }
    }

    pub async fn new(options: CacheOptions) -> Result<Self, CacheError> {
        // Set up database connection using the db_connection module
        let db_pool = database::init(&options.db_path).await?;

        // Create LRU cache
        let max_items = NonZeroUsize::new(MAX_LRU_CACHE_ITEMS)
            .ok_or_else(|| CacheError::InvalidValue("MAX_LRU_CACHE_ITEMS cannot be zero".to_string()))?;
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

    pub async fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
        let expires = ttl.map(|duration| -> Result<i64, CacheError> {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_secs() as i64;
            Ok(now + duration.as_secs() as i64)
        }).transpose()?;

        // Update SQLite
        self.set_in_db(key, value, expires).await?;

        // Update memory cache and track memory usage
        let mut memory_cache = self.memory_cache.lock().map_err(|_| CacheError::MutexLockError)?;
        let mut memory_usage = self.current_memory_usage.lock().map_err(|_| CacheError::MutexLockError)?;

        // Check if key already exists in memory cache
        let _old_value_size = if let Some(old_value) = memory_cache.get(key) {
            // If key exists, subtract old value size from memory usage
            let size = old_value.len() + key.len();
            *memory_usage = memory_usage.saturating_sub(size);
            size
        } else {
            0
        };

        // Add new value size to memory usage
        let new_value = value.to_vec();
        let new_value_size = new_value.len() + key.len();
        *memory_usage += new_value_size;

        // Check if memory usage exceeds the limit and perform lazy eviction
        self.evict_if_memory_pressure(&mut memory_cache, &mut memory_usage);

        // Update memory cache
        memory_cache.put(key.to_string(), new_value);

        Ok(())
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        // Check if key exists in memory cache
        let memory_value = {
            let mut memory_cache = self.memory_cache.lock().map_err(|_| CacheError::MutexLockError)?;
            if let Some(value) = memory_cache.get(key) {
                Some(value.clone())
            } else {
                None
            }
        };

        // Check database for TTL information and value if not in memory
        match self.get_from_db(key).await? {
            Some((db_value, expires)) => {
                // Check if expired
                if let Some(expires) = expires {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)?
                        .as_secs() as i64;
                    if now > expires {
                        // Remove expired entry from database
                        self.delete_from_db(key).await?;

                        // Remove from memory cache if it exists there
                        if memory_value.is_some() {
                            let mut memory_cache = self.memory_cache.lock().map_err(|_| CacheError::MutexLockError)?;
                            let mut memory_usage = self.current_memory_usage.lock().map_err(|_| CacheError::MutexLockError)?;

                            if let Some(value) = memory_cache.pop(key) {
                                // Subtract the size of the key and value from memory usage
                                let size = value.len() + key.len();
                                *memory_usage = memory_usage.saturating_sub(size);
                                println!("Removed expired key '{}' from memory cache during get operation", key);
                            }
                        }

                        return Ok(None);
                    }
                }

                // If we have a memory value, it's valid (not expired) so return it
                if let Some(value) = memory_value {
                    // Update last accessed time
                    self.update_last_accessed(key).await?;
                    return Ok(Some(value));
                }

                // Otherwise, update memory cache with the database value
                let mut memory_cache = self.memory_cache.lock().map_err(|_| CacheError::MutexLockError)?;
                let mut memory_usage = self.current_memory_usage.lock().map_err(|_| CacheError::MutexLockError)?;

                // Add new value size to memory usage
                let new_value = db_value.clone();
                let new_value_size = new_value.len() + key.len();
                *memory_usage += new_value_size;

                // Check if memory usage exceeds the limit and perform lazy eviction
                self.evict_if_memory_pressure(&mut memory_cache, &mut memory_usage);

                // Update memory cache
                memory_cache.put(key.to_string(), new_value);

                // Update last accessed time
                self.update_last_accessed(key).await?;

                Ok(Some(db_value))
            }
            None => {
                // If not in database but in memory (shouldn't happen normally), remove from memory
                if memory_value.is_some() {
                    let mut memory_cache = self.memory_cache.lock().map_err(|_| CacheError::MutexLockError)?;
                    let mut memory_usage = self.current_memory_usage.lock().map_err(|_| CacheError::MutexLockError)?;

                    if let Some(value) = memory_cache.pop(key) {
                        // Subtract the size of the key and value from memory usage
                        let size = value.len() + key.len();
                        *memory_usage = memory_usage.saturating_sub(size);
                        println!("Removed key '{}' from memory cache that was not in database", key);
                    }
                }

                Ok(None)
            },
        }
    }

    pub async fn delete(&self, key: &str) -> Result<(), CacheError> {
        // Remove from memory cache and update memory usage
        {
            let mut memory_cache = self.memory_cache.lock().map_err(|_| CacheError::MutexLockError)?;
            let mut memory_usage = self.current_memory_usage.lock().map_err(|_| CacheError::MutexLockError)?;

            // Get the value being deleted to calculate its size
            if let Some(value) = memory_cache.pop(key) {
                // Subtract the size of the key and value from memory usage
                let size = value.len() + key.len();
                *memory_usage = memory_usage.saturating_sub(size);
            }
        }

        // Remove from database
        self.delete_from_db(key).await?;

        Ok(())
    }

    // Private helper methods

    async fn set_in_db(&self, key: &str, value: &[u8], expires: Option<i64>) -> Result<(), CacheError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs() as i64;

        // Use the retry function to handle busy errors
        let db_pool = self.db_pool.clone();
        retry_on_busy(move || {
            let key = key.to_string();
            let value = value.to_vec();
            let db_pool = db_pool.clone();
            async move {
                // For SQLite, we need to use a different approach for upsert
                println!("Checking if key exists: {}", key);
                let existing = key_exists(&db_pool, &key).await?;

                println!("Key exists: {}", existing);

                if existing {
                    println!("Updating existing key: {}", key);
                    update_cache_entry(&db_pool, &key, &value, expires, now).await?;
                    println!("Update successful");
                } else {
                    println!("Inserting new key: {}", key);
                    insert_cache_entry(&db_pool, &key, &value, expires, now).await?;
                    println!("Insert successful");
                }

                Ok(())
            }
        }).await?;

        Ok(())
    }

    async fn get_from_db(&self, key: &str) -> Result<Option<(Vec<u8>, Option<i64>)>, CacheError> {
        let db_pool = self.db_pool.clone();
        let key_str = key.to_string();

        let result = retry_on_busy(move || {
            let key = key_str.clone();
            let db_pool = db_pool.clone();
            async move {
                get_cache_entry(&db_pool, &key).await
            }
        }).await?;

        match result {
            Some(entry) => {
                Ok(Some((entry.cache_value, entry.expires)))
            },
            None => Ok(None),
        }
    }

    async fn update_last_accessed(&self, key: &str) -> Result<(), CacheError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs() as i64;

        let db_pool = self.db_pool.clone();
        let key_str = key.to_string();

        retry_on_busy(move || {
            let key = key_str.clone();
            let db_pool = db_pool.clone();
            let now = now;
            async move {
                update_last_accessed(&db_pool, &key, now).await
            }
        }).await?;

        Ok(())
    }

    async fn delete_from_db(&self, key: &str) -> Result<(), CacheError> {
        let db_pool = self.db_pool.clone();
        retry_on_busy(move || {
            let key = key.to_string();
            let db_pool = db_pool.clone();
            async move {
                delete_cache_entry(&db_pool, &key).await
            }
        }).await?;

        Ok(())
    }

    /// Cleans up expired keys from the database
    /// 
    /// This method can be called manually to remove expired keys from both the memory cache and the database.
    /// It returns the number of keys that were removed.
    pub async fn cleanup_expired_keys(&self) -> Result<usize, CacheError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs() as i64;

        let db_pool = self.db_pool.clone();

        // Get all expired keys
        let expired_keys = retry_on_busy(move || {
            let db_pool = db_pool.clone();
            let now = now;
            async move {
                get_expired_keys(&db_pool, now).await
            }
        }).await?;

        if expired_keys.is_empty() {
            return Ok(0);
        }

        println!("Found {} expired keys to clean up", expired_keys.len());

        // Extract key strings from CacheKey structs
        let key_strings: Vec<String> = expired_keys.into_iter()
            .map(|k| k.cache_key)
            .collect();

        // Remove expired keys from memory cache
        {
            let mut memory_cache = self.memory_cache.lock().map_err(|_| CacheError::MutexLockError)?;
            let mut memory_usage = self.current_memory_usage.lock().map_err(|_| CacheError::MutexLockError)?;

            for key in &key_strings {
                if let Some(value) = memory_cache.pop(key) {
                    // Subtract the size of the key and value from memory usage
                    let size = value.len() + key.len();
                    *memory_usage = memory_usage.saturating_sub(size);
                    println!("Removed expired key '{}' from memory cache during cleanup", key);
                }
            }
        }

        // Delete expired keys from database in batch
        let db_pool = self.db_pool.clone();
        let count = key_strings.len();

        retry_on_busy(move || {
            let db_pool = db_pool.clone();
            let key_strings = key_strings.clone();
            async move {
                delete_keys_batch(&db_pool, &key_strings).await
            }
        }).await?;

        println!("Cleaned up {} expired keys", count);

        Ok(count)
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
        let db_path = "tests/db/cargo_unit_tests.sqlite".to_string();
        println!("Creating test cache with database path: {}", db_path);

        // Remove the database file if it exists
        if std::path::Path::new(&db_path).exists() {
            std::fs::remove_file(&db_path).expect("Failed to remove existing database file");
            println!("Removed existing database file");
        }

        // Create a cache with a very short cleanup interval
        let options = CacheOptions {
            max_memory_mb: 10,
            db_path,
        };

        let cache = Cache::new(options).await.expect("Failed to create cache for test");
        println!("Successfully created cache instance");

        let key = "lib_rs_test_cleanup_expired_entries";
        let value = b"test_value";

        // Set with 1 second TTL
        println!("Setting key '{}' with 1 second TTL", key);
        cache.set(key, value, Some(Duration::from_secs(1))).await.expect("Failed to set key in test");

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
            let result = cache.get(key).await.expect("Failed to get key in test attempt");
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

        // Create a test cache
        let db_path = "tests/db/cargo_unit_tests_manual_cleanup.sqlite".to_string();
        println!("Creating test cache with database path: {}", db_path);

        // Remove the database file if it exists
        if std::path::Path::new(&db_path).exists() {
            std::fs::remove_file(&db_path).expect("Failed to remove existing database file");
            println!("Removed existing database file");
        }

        // Create a cache
        let options = CacheOptions {
            max_memory_mb: 10,
            db_path,
        };

        // Create a new cache instance
        let cache = Cache::new(options).await.expect("Failed to create cache for test");
        println!("Successfully created cache instance");

        // Add several keys with short TTLs
        for i in 0..5 {
            let key = format!("manual_cleanup_test_key_{}", i);
            let value = format!("value_{}", i).into_bytes();

            // Set with 2 second TTL
            println!("Setting key '{}' with 2 second TTL", key);
            cache.set(&key, &value, Some(Duration::from_secs(2))).await.expect("Failed to set key in test");

            // Verify it was set correctly
            let result = cache.get(&key).await.expect("Failed to get key in test");
            assert_eq!(result, Some(value.clone()));
        }

        // Wait for keys to expire (3 seconds should be enough)
        println!("Waiting for 3 seconds to allow keys to expire...");
        sleep(Duration::from_secs(3)).await;
        println!("Wait complete, keys should now be expired");

        // Manually run the cleanup
        println!("Running manual cleanup");
        let cleaned_count = cache.cleanup_expired_keys().await.expect("Failed to run cleanup");
        println!("Cleaned up {} keys", cleaned_count);
        assert_eq!(cleaned_count, 5, "Should have cleaned up 5 keys");

        // Verify all keys have been removed
        for i in 0..5 {
            let key = format!("manual_cleanup_test_key_{}", i);
            let result = cache.get(&key).await.expect("Failed to get key in test");
            assert_eq!(result, None, "Key '{}' should have been removed by manual cleanup", key);
        }

        println!("SUCCESS: All keys were properly removed by manual cleanup");
    }
}
