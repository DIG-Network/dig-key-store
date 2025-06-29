use std::path::Path;
use sqlx::{sqlite::SqlitePoolOptions, migrate::MigrateDatabase, Sqlite, Pool};
use sqlx::migrate::Migrator;
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
    let migrator = sqlx::migrate!("./migrations");
    match migrator.run(db_connection).await {
        Ok(()) => Ok(()),
        Err(e) => Err(DbError::MigrationError(e.to_string())),
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
