use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::sync::{Arc, Mutex};
use std::num::NonZeroUsize;

use sqlx::{sqlite::SqlitePoolOptions, migrate::MigrateDatabase, Sqlite, Pool, Row};
use lru::LruCache;
use thiserror::Error;
use tokio::time;

// No schema or models needed with sqlx

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
}

#[derive(Debug, Clone)]
pub struct CacheOptions {
    pub max_memory_mb: usize,
    pub db_path: String,
    pub cleanup_interval: Duration,
}

/// A key-value cache with both in-memory and persistent storage.
///
/// The cache stores values in both memory (using an LRU cache) and in a SQLite database.
/// Values can be set with an optional time-to-live (TTL).
///
/// # Examples
///
/// ```
/// use dig_key_value_store::{Cache, CacheOptions};
/// use std::time::Duration;
///
/// #[tokio::main]
/// async fn main() {
///     // Create a cache with default options
///     let options = CacheOptions {max_memory_mb: 100, db_path: "cache.sqlite".to_string(), cleanup_interval: Duration::from_secs(60)};
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
    #[allow(dead_code)]
    options: CacheOptions,
    _cleanup_task: Option<tokio::task::JoinHandle<()>>,
}

impl Cache {
    pub async fn new(options: CacheOptions) -> Result<Self, CacheError> {
        // Calculate max items based on memory limit (rough approximation)
        // Assuming average key size of 50 bytes and value size of 1000 bytes
        let max_items = (options.max_memory_mb * 1024 * 1024) / (50 + 1000);
        let max_items = NonZeroUsize::new(max_items.max(1)).unwrap();

        println!("Setting up database connection pool for: {}", options.db_path);

        // Check if the database exists, if not create it
        let db_url = format!("sqlite:{}", options.db_path);
        if !Sqlite::database_exists(&db_url).await.unwrap_or(false) {
            println!("Database does not exist, creating it");
            Sqlite::create_database(&db_url).await?;
        }

        // Set up database connection pool
        let db_pool = match SqlitePoolOptions::new()
            .max_connections(10)
            .connect(&db_url).await {
            Ok(pool) => {
                println!("Database connection pool created successfully");
                pool
            },
            Err(e) => {
                println!("Error creating database connection pool: {}", e);
                return Err(CacheError::PoolError(e.to_string()));
            }
        };

        // Run migrations by reading from migration files
        println!("Running migrations from files");

        // Read the migration SQL from the file
        println!("Reading migration SQL from file");
        let migration_path = "migrations/20230101000000_create_cache_table/up.sql";
        let create_table_sql = match std::fs::read_to_string(migration_path) {
            Ok(sql) => sql,
            Err(e) => {
                println!("Error reading migration file: {}", e);
                return Err(CacheError::MigrationError(format!("Failed to read migration file: {}", e)));
            }
        };

        // Execute the migration SQL
        println!("Executing migration SQL");
        match sqlx::query(&create_table_sql).execute(&db_pool).await {
            Ok(_) => println!("Migrations executed successfully"),
            Err(e) => {
                println!("Error executing migrations: {}", e);
                return Err(CacheError::MigrationError(e.to_string()));
            }
        };

        // Create LRU cache
        let memory_cache = Arc::new(Mutex::new(LruCache::new(max_items)));

        // Set up cleanup task
        let cleanup_interval = options.cleanup_interval;
        let thread_db_pool = db_pool.clone();
        let thread_memory_cache = Arc::clone(&memory_cache);

        let cleanup_task = tokio::spawn(async move {
            let mut interval = time::interval(cleanup_interval);
            loop {
                interval.tick().await;
                // Clean up expired entries
                let _ = Self::cleanup_expired_entries(&thread_db_pool, &thread_memory_cache).await;
            }
        });

        Ok(Self {
            memory_cache,
            db_pool,
            options,
            _cleanup_task: Some(cleanup_task),
        })
    }

    pub async fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
        let expires = ttl.map(|duration| {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            now + duration.as_secs() as i64
        });

        // Update SQLite
        self.set_in_db(key, value, expires).await?;

        // Update memory cache
        let mut memory_cache = self.memory_cache.lock().unwrap();
        memory_cache.put(key.to_string(), value.to_vec());

        Ok(())
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        // Try memory cache first
        {
            let mut memory_cache = self.memory_cache.lock().unwrap();
            if let Some(value) = memory_cache.get(key) {
                return Ok(Some(value.clone()));
            }
        }

        // If not in memory, try database
        match self.get_from_db(key).await? {
            Some((value, expires)) => {
                // Check if expired
                if let Some(expires) = expires {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64;
                    if now > expires {
                        // Remove expired entry
                        self.delete_from_db(key).await?;
                        return Ok(None);
                    }
                }

                // Update memory cache
                let mut memory_cache = self.memory_cache.lock().unwrap();
                memory_cache.put(key.to_string(), value.clone());

                // Update last accessed time
                self.update_last_accessed(key).await?;

                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    pub async fn delete(&self, key: &str) -> Result<(), CacheError> {
        // Remove from memory cache
        {
            let mut memory_cache = self.memory_cache.lock().unwrap();
            memory_cache.pop(key);
        }

        // Remove from database
        self.delete_from_db(key).await?;

        Ok(())
    }

    // Private helper methods

    async fn set_in_db(&self, key: &str, value: &[u8], expires: Option<i64>) -> Result<(), CacheError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // For SQLite, we need to use a different approach for upsert
        println!("Checking if key exists: {}", key);
        let existing = sqlx::query("SELECT EXISTS(SELECT 1 FROM cache WHERE cache_key = ?)")
            .bind(key)
            .fetch_one(&self.db_pool)
            .await?
            .get::<bool, _>(0);

        println!("Key exists: {}", existing);

        if existing {
            println!("Updating existing key: {}", key);
            sqlx::query(
                "UPDATE cache SET cache_value = ?, expires = ?, last_accessed = ? WHERE cache_key = ?"
            )
            .bind(value)
            .bind(expires)
            .bind(now)
            .bind(key)
            .execute(&self.db_pool)
            .await?;
            println!("Update successful");
        } else {
            println!("Inserting new key: {}", key);
            sqlx::query(
                "INSERT INTO cache (cache_key, cache_value, expires, last_accessed) VALUES (?, ?, ?, ?)"
            )
            .bind(key)
            .bind(value)
            .bind(expires)
            .bind(now)
            .execute(&self.db_pool)
            .await?;
            println!("Insert successful");
        }

        Ok(())
    }

    async fn get_from_db(&self, key: &str) -> Result<Option<(Vec<u8>, Option<i64>)>, CacheError> {
        let result = sqlx::query("SELECT cache_value, expires FROM cache WHERE cache_key = ?")
            .bind(key)
            .fetch_optional(&self.db_pool)
            .await?;

        match result {
            Some(row) => {
                let value: Vec<u8> = row.get("cache_value");
                let expires: Option<i64> = row.get("expires");
                Ok(Some((value, expires)))
            },
            None => Ok(None),
        }
    }

    async fn update_last_accessed(&self, key: &str) -> Result<(), CacheError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        sqlx::query("UPDATE cache SET last_accessed = ? WHERE cache_key = ?")
            .bind(now)
            .bind(key)
            .execute(&self.db_pool)
            .await?;

        Ok(())
    }

    async fn delete_from_db(&self, key: &str) -> Result<(), CacheError> {
        sqlx::query("DELETE FROM cache WHERE cache_key = ?")
            .bind(key)
            .execute(&self.db_pool)
            .await?;

        Ok(())
    }

    async fn cleanup_expired_entries(db_pool: &Pool<Sqlite>, memory_cache: &Arc<Mutex<LruCache<String, Vec<u8>>>>) -> Result<(), CacheError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Get expired keys
        let rows = sqlx::query("SELECT cache_key FROM cache WHERE expires < ? AND expires IS NOT NULL")
            .bind(now)
            .fetch_all(db_pool)
            .await?;

        let expired_keys: Vec<String> = rows.iter()
            .map(|row| row.get("cache_key"))
            .collect();

        // Remove from memory cache
        {
            let mut memory_cache = memory_cache.lock().unwrap();
            for key in &expired_keys {
                memory_cache.pop(key);
            }
        }

        // Remove from database
        if !expired_keys.is_empty() {
            // SQLite doesn't support array parameters, so we need to build a query with placeholders
            let placeholders = expired_keys.iter()
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(",");

            let query = format!("DELETE FROM cache WHERE cache_key IN ({})", placeholders);

            let mut query_builder = sqlx::query(&query);
            for key in &expired_keys {
                query_builder = query_builder.bind(key);
            }

            query_builder.execute(db_pool).await?;
        }

        Ok(())
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
        let test_name = std::thread::current().name().unwrap_or("unknown").to_string();
        let db_path = format!("tests/db/test_cache_unit_{}.sqlite", test_name);
        println!("Creating test cache with database path: {}", db_path);

        // Ensure the tests/db directory exists
        std::fs::create_dir_all("tests/db").expect("Failed to create tests/db directory");
        println!("Created tests/db directory if it didn't exist");

        // Remove the database file if it exists
        if std::path::Path::new(&db_path).exists() {
            std::fs::remove_file(&db_path).expect("Failed to remove existing database file");
            println!("Removed existing database file");
        }

        // Create a cache with a very short cleanup interval
        let options = CacheOptions {
            max_memory_mb: 10,
            db_path,
            cleanup_interval: Duration::from_millis(100), // Very short interval to trigger cleanup quickly
        };
        println!("Configured cache with 100ms cleanup interval");

        let cache = Cache::new(options).await.unwrap();
        println!("Successfully created cache instance");

        let key = "test_key";
        let value = b"test_value";

        // Set with 1 second TTL
        println!("Setting key '{}' with 1 second TTL", key);
        cache.set(key, value, Some(Duration::from_secs(1))).await.unwrap();

        // Should be available immediately
        println!("Verifying key is available immediately after setting");
        let result = cache.get(key).await.unwrap();
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
            let result = cache.get(key).await.unwrap();
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
}
