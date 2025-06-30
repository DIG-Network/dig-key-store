use std::path::Path;
use sqlx::{sqlite::SqlitePoolOptions, migrate::{MigrateDatabase, Migrator}, Sqlite, Pool, FromRow};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("Database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    #[error("Migration error: {0}")]
    MigrationError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Pool error: {0}")]
    PoolError(String),
}

pub async fn init(db_path: &str) -> Result<Pool<Sqlite>, DbError> {
    let db_connection = setup_db_connection(db_path).await?;
    run_migrations(&db_connection).await?;
    Ok(db_connection)
}

async fn run_migrations(db_connection: &Pool<Sqlite>) -> Result<(), DbError> {
    println!("Running migrations");

    // Check if the cache table already exists
    let table_exists = sqlx::query("SELECT name FROM sqlite_master WHERE type='table' AND name='cache'")
        .fetch_optional(db_connection)
        .await
        .map_err(|e| DbError::DatabaseError(e))?
        .is_some();

    if table_exists {
        println!("Cache table already exists");
        return Ok(());
    }

    // Define the migrations path
    let migrations_path = Path::new("./migrations");

    // Try running the migrations using the SQLx API first
    let migrator = match Migrator::new(migrations_path).await {
        Ok(migrator) => migrator,
        Err(e) => {
            println!("Error creating migrator: {}", e);
            return Err(DbError::MigrationError(format!("Failed to create migrator: {}", e)));
        }
    };

    if let Ok(_) = migrator.run(db_connection).await {
        println!("Migrations completed successfully via SQLx API");

        // Verify that the cache table exists after migrations
        let table_exists = sqlx::query("SELECT name FROM sqlite_master WHERE type='table' AND name='cache'")
            .fetch_optional(db_connection)
            .await
            .map_err(|e| DbError::DatabaseError(e))?
            .is_some();

        if table_exists {
            println!("Cache table created by SQLx migrations");
            return Ok(());
        }
    }

    // If SQLx migrations didn't create the table, run the SQL directly
    println!("Running SQL migration directly");

    // Read the SQL from the migration file
    let up_sql_path = migrations_path.join("20230101000000_create_cache_table").join("up.sql");
    let sql = std::fs::read_to_string(&up_sql_path)
        .map_err(|e| {
            println!("Error reading migration file: {}", e);
            DbError::IoError(e)
        })?;

    // Execute the SQL directly
    sqlx::query(&sql)
        .execute(db_connection)
        .await
        .map_err(|e| {
            println!("Error executing SQL directly: {}", e);
            DbError::DatabaseError(e)
        })?;

    // Verify that the cache table exists now
    let table_exists = sqlx::query("SELECT name FROM sqlite_master WHERE type='table' AND name='cache'")
        .fetch_optional(db_connection)
        .await
        .map_err(|e| DbError::DatabaseError(e))?
        .is_some();

    if table_exists {
        println!("Cache table created successfully");
        Ok(())
    } else {
        println!("Failed to create cache table");
        Err(DbError::MigrationError("Failed to create cache table".to_string()))
    }
}

/// Sets up a SQLite database connection pool
async fn setup_db_connection(db_path: &str) -> Result<Pool<Sqlite>, DbError> {
    println!("Setting up database connection pool for: {}", db_path);

    // Ensure the directory for the database exists
    if let Some(parent) = Path::new(db_path).parent() {
        if !parent.exists() {
            println!("Creating directory for database: {:?}", parent);
            std::fs::create_dir_all(parent)?;
        }
    }

    // Check if the database exists, if not create it
    let db_url = format!("sqlite:{}", db_path);
    if !Sqlite::database_exists(&db_url).await.unwrap_or(false) {
        println!("Database does not exist, creating it");
        Sqlite::create_database(&db_url).await?;
    }

    // Set up database connection pool with 10-second timeout
    let db_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(&db_url).await
        .map_err(|e| {
            println!("Error creating database connection pool: {}", e);
            DbError::PoolError(e.to_string())
        })?;

    println!("Database connection pool created successfully");

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
    let result = sqlx::query_as::<_, KeyExistsResult>("SELECT EXISTS(SELECT 1 FROM cache WHERE cache_key = ?) as \"exists\"")
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
    last_accessed: i64
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE cache SET cache_value = ?, expires = ?, last_accessed = ? WHERE cache_key = ?")
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
    last_accessed: i64
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO cache (cache_key, cache_value, expires, last_accessed) VALUES (?, ?, ?, ?)")
        .bind(key)
        .bind(value)
        .bind(expires)
        .bind(last_accessed)
        .execute(db_pool)
        .await?;

    Ok(())
}

pub async fn get_cache_entry(db_pool: &Pool<Sqlite>, key: &str) -> Result<Option<CacheEntry>, sqlx::Error> {
    sqlx::query_as::<_, CacheEntry>("SELECT cache_value, expires FROM cache WHERE cache_key = ?")
        .bind(key)
        .fetch_optional(db_pool)
        .await
}

pub async fn update_last_accessed(db_pool: &Pool<Sqlite>, key: &str, last_accessed: i64) -> Result<(), sqlx::Error> {
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

pub async fn get_expired_keys(db_pool: &Pool<Sqlite>, now: i64) -> Result<Vec<CacheKey>, sqlx::Error> {
    sqlx::query_as::<_, CacheKey>("SELECT cache_key FROM cache WHERE expires < ? AND expires IS NOT NULL")
        .bind(now)
        .fetch_all(db_pool)
        .await
}

pub async fn delete_keys_batch(db_pool: &Pool<Sqlite>, keys: &[String]) -> Result<(), sqlx::Error> {
    if keys.is_empty() {
        return Ok(());
    }

    // SQLite doesn't support array parameters, so we need to build a query with placeholders
    let placeholders = keys.iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(",");

    let query = format!("DELETE FROM cache WHERE cache_key IN ({})", placeholders);

    let mut query_builder = sqlx::query(&query);
    for key in keys {
        query_builder = query_builder.bind(key);
    }

    query_builder.execute(db_pool).await?;

    Ok(())
}
