use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::sync::{Arc, Mutex};
use std::thread;
use std::num::NonZeroUsize;

use diesel::prelude::*;
use diesel::r2d2::{self, ConnectionManager};
use diesel::sqlite::SqliteConnection;
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use lru::LruCache;
use thiserror::Error;

pub mod schema;
pub mod models;

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

type DbPool = r2d2::Pool<ConnectionManager<SqliteConnection>>;
type DbConnection = r2d2::PooledConnection<ConnectionManager<SqliteConnection>>;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("Database error: {0}")]
    DatabaseError(#[from] diesel::result::Error),

    #[error("Connection pool error: {0}")]
    ConnectionError(#[from] diesel::r2d2::Error),

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

impl Default for CacheOptions {
    fn default() -> Self {
        Self {
            max_memory_mb: 100,
            db_path: String::from("cache.db"),
            cleanup_interval: Duration::from_secs(60 * 30), // 30 minutes
        }
    }
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
/// // Create a cache with default options
/// let options = CacheOptions::default();
/// let cache = Cache::new(options).expect("Failed to create cache");
///
/// // Set a value
/// let key = "example_key";
/// let value = b"example_value";
/// cache.set(key, value, None).expect("Failed to set value");
///
/// // Get a value
/// let result = cache.get(key).expect("Failed to get value");
/// assert_eq!(result, Some(value.to_vec()));
/// ```
pub struct Cache {
    memory_cache: Arc<Mutex<LruCache<String, Vec<u8>>>>,
    db_pool: DbPool,
    #[allow(dead_code)]
    options: CacheOptions,
    _cleanup_thread: Option<thread::JoinHandle<()>>,
}

impl Cache {
    pub fn new(options: CacheOptions) -> Result<Self, CacheError> {
        // Calculate max items based on memory limit (rough approximation)
        // Assuming average key size of 50 bytes and value size of 1000 bytes
        let max_items = (options.max_memory_mb * 1024 * 1024) / (50 + 1000);
        let max_items = NonZeroUsize::new(max_items.max(1)).unwrap();

        println!("Setting up database connection pool for: {}", options.db_path);

        // Check if the migrations directory exists
        let migrations_dir = std::path::Path::new("migrations");
        if migrations_dir.exists() {
            println!("Migrations directory exists");
        } else {
            println!("Migrations directory does not exist");
            // Print the current directory
            println!("Current directory: {:?}", std::env::current_dir().unwrap());
            // List files in the current directory
            println!("Files in current directory:");
            for entry in std::fs::read_dir(".").unwrap() {
                let entry = entry.unwrap();
                println!("  {:?}", entry.path());
            }
        }

        // Set up database connection pool
        let manager = ConnectionManager::<SqliteConnection>::new(&options.db_path);
        let db_pool = match r2d2::Pool::builder()
            .max_size(10)
            .build(manager) {
            Ok(pool) => {
                println!("Database connection pool created successfully");
                pool
            },
            Err(e) => {
                println!("Error creating database connection pool: {}", e);
                return Err(CacheError::PoolError(e.to_string()));
            }
        };

        // Run migrations
        println!("Getting connection from pool");
        let mut conn = match db_pool.get() {
            Ok(conn) => {
                println!("Got connection from pool");
                conn
            },
            Err(e) => {
                println!("Error getting connection from pool: {}", e);
                return Err(CacheError::PoolError(e.to_string()));
            }
        };

        println!("Running migrations");
        match conn.run_pending_migrations(MIGRATIONS) {
            Ok(_) => println!("Migrations run successfully"),
            Err(e) => {
                println!("Error running migrations: {}", e);
                return Err(CacheError::MigrationError(e.to_string()));
            }
        };

        // Create LRU cache
        let memory_cache = Arc::new(Mutex::new(LruCache::new(max_items)));

        // Set up cleanup thread
        let cleanup_interval = options.cleanup_interval;
        let thread_db_pool = db_pool.clone();
        let thread_memory_cache = Arc::clone(&memory_cache);

        let cleanup_thread = thread::spawn(move || {
            loop {
                thread::sleep(cleanup_interval);
                if let Ok(mut conn) = thread_db_pool.get() {
                    // Clean up expired entries
                    let _ = Self::cleanup_expired_entries(&mut conn, &thread_memory_cache);
                }
            }
        });

        Ok(Self {
            memory_cache,
            db_pool,
            options,
            _cleanup_thread: Some(cleanup_thread),
        })
    }

    pub fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
        let expires = ttl.map(|duration| {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            now + duration.as_secs() as i64
        });

        // Update SQLite
        let mut conn = self.db_pool.get()
            .map_err(|e| CacheError::PoolError(e.to_string()))?;
        self.set_in_db(&mut conn, key, value, expires)?;

        // Update memory cache
        let mut memory_cache = self.memory_cache.lock().unwrap();
        memory_cache.put(key.to_string(), value.to_vec());

        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        // Try memory cache first
        {
            let mut memory_cache = self.memory_cache.lock().unwrap();
            if let Some(value) = memory_cache.get(key) {
                return Ok(Some(value.clone()));
            }
        }

        // If not in memory, try database
        let mut conn = self.db_pool.get()
            .map_err(|e| CacheError::PoolError(e.to_string()))?;
        match self.get_from_db(&mut conn, key)? {
            Some((value, expires)) => {
                // Check if expired
                if let Some(expires) = expires {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64;
                    if now > expires {
                        // Remove expired entry
                        self.delete_from_db(&mut conn, key)?;
                        return Ok(None);
                    }
                }

                // Update memory cache
                let mut memory_cache = self.memory_cache.lock().unwrap();
                memory_cache.put(key.to_string(), value.clone());

                // Update last accessed time
                self.update_last_accessed(&mut conn, key)?;

                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    pub fn delete(&self, key: &str) -> Result<(), CacheError> {
        // Remove from memory cache
        {
            let mut memory_cache = self.memory_cache.lock().unwrap();
            memory_cache.pop(key);
        }

        // Remove from database
        let mut conn = self.db_pool.get()
            .map_err(|e| CacheError::PoolError(e.to_string()))?;
        self.delete_from_db(&mut conn, key)?;

        Ok(())
    }

    // Private helper methods

    fn set_in_db(&self, conn: &mut DbConnection, key: &str, value: &[u8], expires: Option<i64>) -> Result<(), CacheError> {
        use self::schema::cache;
        use self::models::NewCacheEntry;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let new_entry = NewCacheEntry {
            cache_key: key,
            cache_value: value,
            expires: expires.as_ref(),
            last_accessed: &now,
        };

        // For SQLite, we need to use a different approach for upsert
        println!("Checking if key exists: {}", key);
        let existing = match diesel::select(diesel::dsl::exists(
            cache::table.filter(cache::cache_key.eq(key))
        )).get_result::<bool>(conn) {
            Ok(result) => result,
            Err(e) => {
                println!("Error checking if key exists: {:?}", e);
                return Err(CacheError::DatabaseError(e));
            }
        };

        println!("Key exists: {}", existing);

        if existing {
            println!("Updating existing key: {}", key);
            match diesel::update(cache::table.filter(cache::cache_key.eq(key)))
                .set((
                    cache::cache_value.eq(value),
                    cache::expires.eq(expires),
                    cache::last_accessed.eq(now),
                ))
                .execute(conn) {
                Ok(_) => println!("Update successful"),
                Err(e) => {
                    println!("Error updating key: {:?}", e);
                    return Err(CacheError::DatabaseError(e));
                }
            };
        } else {
            println!("Inserting new key: {}", key);
            match diesel::insert_into(cache::table)
                .values(&new_entry)
                .execute(conn) {
                Ok(_) => println!("Insert successful"),
                Err(e) => {
                    println!("Error inserting key: {:?}", e);
                    return Err(CacheError::DatabaseError(e));
                }
            };
        }

        Ok(())
    }

    fn get_from_db(&self, conn: &mut DbConnection, key: &str) -> Result<Option<(Vec<u8>, Option<i64>)>, CacheError> {
        use self::schema::cache::dsl::*;

        let result = cache
            .filter(cache_key.eq(key))
            .select((cache_value, expires))
            .first::<(Vec<u8>, Option<i64>)>(conn)
            .optional()?;

        Ok(result)
    }

    fn update_last_accessed(&self, conn: &mut DbConnection, key: &str) -> Result<(), CacheError> {
        use self::schema::cache::dsl::*;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        diesel::update(cache.filter(cache_key.eq(key)))
            .set(last_accessed.eq(now))
            .execute(conn)?;

        Ok(())
    }

    fn delete_from_db(&self, conn: &mut DbConnection, key: &str) -> Result<(), CacheError> {
        use self::schema::cache::dsl::*;

        diesel::delete(cache.filter(cache_key.eq(key)))
            .execute(conn)?;

        Ok(())
    }

    fn cleanup_expired_entries(conn: &mut DbConnection, memory_cache: &Arc<Mutex<LruCache<String, Vec<u8>>>>) -> Result<(), CacheError> {
        use self::schema::cache::dsl::*;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Get expired keys
        let expired_keys: Vec<String> = cache
            .filter(expires.lt(now).and(expires.is_not_null()))
            .select(cache_key)
            .load::<String>(conn)?;

        // Remove from memory cache
        {
            let mut memory_cache = memory_cache.lock().unwrap();
            for key in &expired_keys {
                memory_cache.pop(key);
            }
        }

        // Remove from database
        if !expired_keys.is_empty() {
            diesel::delete(cache.filter(cache_key.eq_any(&expired_keys)))
                .execute(conn)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_cleanup_expired_entries() {
        println!("UNIT TEST: Testing cleanup of expired entries");

        // Create a test cache
        let test_name = std::thread::current().name().unwrap_or("unknown").to_string();
        let db_path = format!("tests/db/test_cache_unit_{}.db", test_name);
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

        let cache = Cache::new(options).unwrap();
        println!("Successfully created cache instance");

        let key = "test_key";
        let value = b"test_value";

        // Set with 1 second TTL
        println!("Setting key '{}' with 1 second TTL", key);
        cache.set(key, value, Some(Duration::from_secs(1))).unwrap();

        // Should be available immediately
        println!("Verifying key is available immediately after setting");
        let result = cache.get(key).unwrap();
        assert_eq!(result, Some(value.to_vec()));
        println!("Key was successfully retrieved immediately after setting");

        // Wait for expiration and cleanup
        println!("Waiting for 2 seconds to allow key to expire...");
        thread::sleep(Duration::from_secs(2));
        println!("Wait complete, key should now be expired");

        // Call get multiple times to ensure the expiration check is triggered
        // The first call might not trigger the check if the value is still in the memory cache
        println!("Attempting to retrieve expired key (may require multiple attempts)");
        for attempt in 1..=3 {
            println!("Attempt #{} to verify key has expired", attempt);
            let result = cache.get(key).unwrap();
            if result.is_none() {
                // Test passes if we get None
                println!("SUCCESS: Key has expired and was properly removed from cache");
                return;
            }
            // Wait a bit before trying again
            println!("Key still exists in cache, waiting 500ms before next attempt");
            thread::sleep(Duration::from_millis(500));
        }

        // If we get here, the test fails
        panic!("Value did not expire after multiple attempts");
    }
}

