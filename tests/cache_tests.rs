use std::time::Duration;
use dig_key_value_store::{Cache, CacheOptions};
use tokio::time::sleep;

async fn create_test_cache() -> Cache {
    // Use a unique path for each test
    let test_name = std::thread::current().name().unwrap_or("unknown").to_string();
    let db_path = format!("tests/db/test_cache_{}.sqlite", test_name);
    println!("Using database path: {}", db_path);

    // Ensure the tests/db directory exists
    std::fs::create_dir_all("tests/db").expect("Failed to create tests/db directory");
    println!("Created tests/db directory if it didn't exist");

    // Remove the database file if it exists
    if std::path::Path::new(&db_path).exists() {
        std::fs::remove_file(&db_path).expect("Failed to remove existing database file");
        println!("Removed existing database file");
    }

    let options = CacheOptions {
        max_memory_mb: 10,
        db_path,
        cleanup_interval: Duration::from_millis(100), // Very short interval to trigger cleanup quickly
    };
    println!("Configured cache with 100ms cleanup interval");

    match Cache::new(options).await {
        Ok(cache) => {
            println!("Successfully created cache instance");
            cache
        },
        Err(e) => {
            panic!("Failed to create cache: {:?}", e);
        }
    }
}

#[tokio::test]
async fn test_set_get() {
    println!("\nINTEGRATION TEST: Testing set and get operations");

    let cache = create_test_cache().await;
    let key = "test_key";
    let value = b"test_value";

    println!("Setting key '{}' with no expiration", key);
    cache.set(key, value, None).await.unwrap();

    println!("Retrieving key '{}'", key);
    let result = cache.get(key).await.unwrap();

    assert_eq!(result, Some(value.to_vec()));
    println!("SUCCESS: Key was successfully set and retrieved");
}

#[tokio::test]
async fn test_delete() {
    println!("\nINTEGRATION TEST: Testing delete operation");

    let cache = create_test_cache().await;
    let key = "test_key";
    let value = b"test_value";

    println!("Setting key '{}' with no expiration", key);
    cache.set(key, value, None).await.unwrap();

    println!("Deleting key '{}'", key);
    cache.delete(key).await.unwrap();

    println!("Attempting to retrieve deleted key '{}'", key);
    let result = cache.get(key).await.unwrap();

    assert_eq!(result, None);
    println!("SUCCESS: Key was successfully deleted and not found in cache");
}

#[tokio::test]
async fn test_expiration() {
    println!("\nINTEGRATION TEST: Testing key expiration");

    let cache = create_test_cache().await;
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

    // Wait for expiration
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
