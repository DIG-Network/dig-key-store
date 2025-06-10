use std::path::Path;
use sqlx::{sqlite::SqlitePoolOptions, migrate::MigrateDatabase, Sqlite, Pool};
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


/// Sets up a SQLite database connection pool
pub async fn setup_db_connection(db_path: &str) -> Result<Pool<Sqlite>, DbError> {
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
        .max_connections(10)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(&db_url).await
        .map_err(|e| {
            println!("Error creating database connection pool: {}", e);
            DbError::PoolError(e.to_string())
        })?;

    println!("Database connection pool created successfully");

    // Run migrations by reading from migration files
    println!("Running migrations from files");

    // Read the migration SQL from the file
    println!("Reading migration SQL from file");
    let migration_path = "migrations/20230101000000_create_cache_table/up.sql";
    let create_table_sql = std::fs::read_to_string(migration_path)
        .map_err(|e| {
            println!("Error reading migration file: {}", e);
            DbError::MigrationError(format!("Failed to read migration file: {}", e))
        })?;

    // Execute the migration SQL
    println!("Executing migration SQL");
    sqlx::query(&create_table_sql).execute(&db_pool).await
        .map_err(|e| {
            println!("Error executing migrations: {}", e);
            DbError::MigrationError(e.to_string())
        })?;

    println!("Migrations executed successfully");

    Ok(db_pool)
}
