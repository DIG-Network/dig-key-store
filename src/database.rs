use crate::is_sqlite_busy_error;
use sqlx::{
    Error, FromRow, Pool, Sqlite,
    migrate::{MigrateDatabase, Migrator},
    sqlite::SqlitePoolOptions,
};
use std::path::Path;
use thiserror::Error;

static MIGRATOR: Migrator = sqlx::migrate!();

#[derive(Debug, Error)]
pub enum DbError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Pool error: {0}")]
    Pool(String),
}

pub async fn init(db_path: &str) -> Result<Pool<Sqlite>, DbError> {
    // Maximum number of retries for the entire initialization process
    let max_init_retries = 10;
    let mut init_retry_count = 0;

    // Retry the entire initialization process if needed
    loop {
        if init_retry_count > 0 {
            println!(
                "Retrying entire database initialization (attempt {}/{})",
                init_retry_count, max_init_retries
            );
            tokio::time::sleep(std::time::Duration::from_millis(100 * init_retry_count)).await;
        }

        let db_connection = match setup_db_connection(db_path).await {
            Ok(conn) => conn,
            Err(e) => {
                eprintln!("Error setting up database connection: {:?}", e);
                init_retry_count += 1;
                if init_retry_count >= max_init_retries {
                    return Err(e);
                }
                continue;
            }
        };

        // First, check if the cache table already exists
        // If it does, we can skip migrations entirely
        let cache_table_exists =
            match sqlx::query("SELECT name FROM sqlite_master WHERE type='table' AND name='cache'")
                .fetch_optional(&db_connection)
                .await
            {
                Ok(result) => result.is_some(),
                Err(e) => {
                    eprintln!("Error checking if cache table exists: {:?}", e);
                    if is_sqlite_busy_error(&e) {
                        init_retry_count += 1;
                        if init_retry_count >= max_init_retries {
                            return Err(DbError::Database(e));
                        }
                        continue;
                    }
                    return Err(DbError::Database(e));
                }
            };

        if cache_table_exists {
            return Ok(db_connection);
        }

        // Check if the _sqlx_migrations table exists
        match sqlx::query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='_sqlx_migrations'",
        )
        .fetch_optional(&db_connection)
        .await
        {
            Ok(result) => result.is_some(),
            Err(e) => {
                eprintln!("Error checking if migrations table exists: {:?}", e);
                if is_sqlite_busy_error(&e) {
                    init_retry_count += 1;
                    if init_retry_count >= max_init_retries {
                        return Err(DbError::Database(e));
                    }
                    continue;
                }
                return Err(DbError::Database(e));
            }
        };

        // Try to run migrations
        match MIGRATOR.run(&db_connection).await {
            Ok(_) => {
                // Verify that the cache table was created
                let cache_table_created = match sqlx::query(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='cache'",
                )
                .fetch_optional(&db_connection)
                .await
                {
                    Ok(result) => result.is_some(),
                    Err(e) => {
                        eprintln!("Error checking if cache table was created: {:?}", e);
                        if is_sqlite_busy_error(&e) {
                            init_retry_count += 1;
                            if init_retry_count >= max_init_retries {
                                return Err(DbError::Database(e));
                            }
                            continue;
                        }
                        return Err(DbError::Database(e));
                    }
                };

                if cache_table_created {
                    return Ok(db_connection);
                } else {
                    eprintln!("Warning: Migrations succeeded but cache table wasn't created");
                    init_retry_count += 1;
                    if init_retry_count >= max_init_retries {
                        return Err(DbError::Pool(
                            "Migrations succeeded but cache table wasn't created".to_string(),
                        ));
                    }
                    continue;
                }
            }
            Err(migration_error) => {
                let sqlx_error = Error::from(migration_error);

                // If it's a busy error, retry the whole process
                if is_sqlite_busy_error(&sqlx_error) {
                    println!("Database is busy during migration, retrying entire initialization");
                    init_retry_count += 1;
                    if init_retry_count >= max_init_retries {
                        return Err(DbError::Database(sqlx_error));
                    }
                    continue;
                }

                // If it's a unique constraint error, it likely means migrations have already been applied
                if let sqlx::Error::Database(db_err) = &sqlx_error {
                    println!("Database error during migration: {:?}", db_err);

                    if let Some(code) = db_err.code() {
                        // SQLite error code 1555 is "UNIQUE constraint failed"
                        if code == "1555" {
                            println!(
                                "Unique constraint error during migration: {}",
                                db_err.message()
                            );

                            // Wait a bit to let any concurrent migrations finish
                            tokio::time::sleep(std::time::Duration::from_millis(200)).await;

                            // Check if the cache table exists now
                            let cache_table_exists = match sqlx::query("SELECT name FROM sqlite_master WHERE type='table' AND name='cache'")
                                .fetch_optional(&db_connection)
                                .await
                            {
                                Ok(result) => result.is_some(),
                                Err(e) => {
                                    eprintln!("Error checking if cache table exists after constraint error: {:?}", e);
                                    if is_sqlite_busy_error(&e) {
                                        init_retry_count += 1;
                                        if init_retry_count >= max_init_retries {
                                            return Err(DbError::Database(e));
                                        }
                                        continue;
                                    }
                                    return Err(DbError::Database(e));
                                }
                            };

                            if cache_table_exists {
                                return Ok(db_connection);
                            } else {
                                eprintln!(
                                    "Cache table doesn't exist after constraint error, retrying entire initialization"
                                );
                                init_retry_count += 1;
                                if init_retry_count >= max_init_retries {
                                    return Err(DbError::Database(sqlx_error));
                                }
                                continue;
                            }
                        }
                    }
                }

                // For any other error, retry the whole process
                eprintln!("Error during migration: {:?}", sqlx_error);
                init_retry_count += 1;
                if init_retry_count >= max_init_retries {
                    return Err(DbError::Database(sqlx_error));
                }
                continue;
            }
        }
    }
}

/// Sets up a SQLite database connection pool
async fn setup_db_connection(db_path: &str) -> Result<Pool<Sqlite>, DbError> {
    // Ensure the directory for the database exists
    if let Some(parent) = Path::new(db_path).parent()
        && !parent.exists()
    {
        std::fs::create_dir_all(parent)?;
    }

    // Check if the database exists, if not create it
    // Use a basic URL first to check existence and create the database
    let basic_db_url = format!("sqlite:{}", db_path);

    // Add retry logic for database creation to handle concurrent access
    let max_retries = 10;
    let mut retry_count = 0;

    while !Sqlite::database_exists(&basic_db_url)
        .await
        .unwrap_or(false)
    {
        match Sqlite::create_database(&basic_db_url).await {
            Ok(_) => {
                break;
            }
            Err(e) => {
                // If it's a busy/locked error, retry
                if is_sqlite_busy_error(&e) {
                    retry_count += 1;
                    if retry_count >= max_retries {
                        return Err(DbError::Database(e));
                    }
                    eprintln!(
                        "Database is busy/locked during creation, retrying in 10ms (attempt {}/{})",
                        retry_count, max_retries
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

                    // Check again if the database exists (another process might have created it)
                    if Sqlite::database_exists(&basic_db_url)
                        .await
                        .unwrap_or(false)
                    {
                        break;
                    }
                } else {
                    // For any other error, return it
                    return Err(DbError::Database(e));
                }
            }
        }
    }

    // Now use a URL with pragmas optimized for concurrent access
    // SQLx uses a different format for SQLite connection URLs with pragmas
    let db_url = format!("sqlite:{}", db_path);

    // Set up database connection pool with 10-second timeout
    // Use more connections to allow concurrent access from multiple instances
    // Add retry logic for connection to handle concurrent access
    let max_retries = 10;
    let mut retry_count = 0;
    let mut db_pool = None;

    while db_pool.is_none() && retry_count < max_retries {
        match SqlitePoolOptions::new()
            .max_connections(5)
            .acquire_timeout(std::time::Duration::from_secs(10))
            .connect(&db_url)
            .await
        {
            Ok(pool) => {
                db_pool = Some(pool);
            }
            Err(e) => {
                // If it's a busy/locked error, retry
                if is_sqlite_busy_error(&e) {
                    retry_count += 1;
                    println!(
                        "Database is busy/locked during connection, retrying in 10ms (attempt {}/{})",
                        retry_count, max_retries
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                } else {
                    // For any other error, return it
                    eprintln!("Error creating database connection pool: {}", e);
                    return Err(DbError::Pool(e.to_string()));
                }
            }
        }
    }

    // If we've exhausted retries and still don't have a connection, return an error
    let db_pool = db_pool.ok_or_else(|| {
        let msg = format!(
            "Failed to connect to database after {} retries",
            max_retries
        );
        println!("{}", msg);
        DbError::Pool(msg)
    })?;

    // Configure SQLite for better concurrent access
    // Execute pragmas to set journal mode to WAL and other optimizations
    sqlx::query("PRAGMA journal_mode = WAL;")
        .execute(&db_pool)
        .await
        .map_err(|e| {
            println!("Error setting journal_mode pragma: {}", e);
            DbError::Database(e)
        })?;

    sqlx::query("PRAGMA synchronous = NORMAL;")
        .execute(&db_pool)
        .await
        .map_err(|e| {
            println!("Error setting synchronous pragma: {}", e);
            DbError::Database(e)
        })?;

    sqlx::query("PRAGMA cache_size = 1000;")
        .execute(&db_pool)
        .await
        .map_err(|e| {
            println!("Error setting cache_size pragma: {}", e);
            DbError::Database(e)
        })?;

    sqlx::query("PRAGMA busy_timeout = 50;")
        .execute(&db_pool)
        .await
        .map_err(|e| {
            println!("Error setting busy_timeout pragma: {}", e);
            DbError::Database(e)
        })?;

    Ok(db_pool)
}

// Type-safe query result structs
#[derive(FromRow)]
pub struct KeyExistsResult {
    pub exists: bool,
}

#[derive(FromRow)]
pub struct CacheEntry {
    pub cache_value: Vec<u8>,
    pub expires: Option<i64>,
}

#[derive(FromRow)]
pub struct CacheKey {
    pub cache_key: String,
}

// Type-safe query functions
pub async fn key_exists(db_pool: &Pool<Sqlite>, key: &str) -> Result<bool, sqlx::Error> {
    let result = sqlx::query_as::<_, KeyExistsResult>(
        "SELECT EXISTS(SELECT 1 FROM cache WHERE cache_key = ?) as \"exists\"",
    )
    .bind(key)
    .fetch_one(db_pool)
    .await?;

    Ok(result.exists)
}

pub async fn update_cache_entry(
    db_pool: &Pool<Sqlite>,
    key: &str,
    value: &[u8],
    expires: Option<i64>,
    last_accessed: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE cache SET cache_value = ?, expires = ?, last_accessed = ? WHERE cache_key = ?",
    )
    .bind(value)
    .bind(expires)
    .bind(last_accessed)
    .bind(key)
    .execute(db_pool)
    .await?;

    Ok(())
}

pub async fn insert_cache_entry(
    db_pool: &Pool<Sqlite>,
    key: &str,
    value: &[u8],
    expires: Option<i64>,
    last_accessed: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO cache (cache_key, cache_value, expires, last_accessed) VALUES (?, ?, ?, ?)",
    )
    .bind(key)
    .bind(value)
    .bind(expires)
    .bind(last_accessed)
    .execute(db_pool)
    .await?;

    Ok(())
}

pub async fn get_cache_entry(
    db_pool: &Pool<Sqlite>,
    key: &str,
) -> Result<Option<CacheEntry>, sqlx::Error> {
    sqlx::query_as::<_, CacheEntry>("SELECT cache_value, expires FROM cache WHERE cache_key = ?")
        .bind(key)
        .fetch_optional(db_pool)
        .await
}

pub async fn update_last_accessed(
    db_pool: &Pool<Sqlite>,
    key: &str,
    last_accessed: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE cache SET last_accessed = ? WHERE cache_key = ?")
        .bind(last_accessed)
        .bind(key)
        .execute(db_pool)
        .await?;

    Ok(())
}

pub async fn delete_cache_entry(db_pool: &Pool<Sqlite>, key: &str) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM cache WHERE cache_key = ?")
        .bind(key)
        .execute(db_pool)
        .await?;

    Ok(())
}

pub async fn get_expired_keys(
    db_pool: &Pool<Sqlite>,
    now: i64,
) -> Result<Vec<CacheKey>, sqlx::Error> {
    sqlx::query_as::<_, CacheKey>(
        "SELECT cache_key FROM cache WHERE expires < ? AND expires IS NOT NULL",
    )
    .bind(now)
    .fetch_all(db_pool)
    .await
}

pub async fn delete_keys_batch(db_pool: &Pool<Sqlite>, keys: &[String]) -> Result<(), sqlx::Error> {
    if keys.is_empty() {
        return Ok(());
    }

    // SQLite doesn't support array parameters, so we need to build a query with placeholders
    let placeholders = keys.iter().map(|_| "?").collect::<Vec<_>>().join(",");

    let query = format!("DELETE FROM cache WHERE cache_key IN ({})", placeholders);

    let mut query_builder = sqlx::query(&query);
    for key in keys {
        query_builder = query_builder.bind(key);
    }

    query_builder.execute(db_pool).await?;

    Ok(())
}
