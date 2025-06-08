use std::time::Duration;
use std::thread;
use dig_key_value_store::{Cache, CacheOptions};

fn create_test_cache() -> Cache {
    // Use a unique path for each test
    let test_name = std::thread::current().name().unwrap_or("unknown").to_string();
    let db_path = format!("test_cache_{}.db", test_name);
    println!("Using database path: {}", db_path);

    // Remove the database file if it exists
    if std::path::Path::new(&db_path).exists() {
        std::fs::remove_file(&db_path).expect("Failed to remove existing database file");
    }

    let options = CacheOptions {
        max_memory_mb: 10,
        db_path,
        cleanup_interval: Duration::from_secs(1),
    };

    match Cache::new(options) {
        Ok(cache) => cache,
        Err(e) => {
            panic!("Failed to create cache: {:?}", e);
        }
    }
}

#[test]
fn test_set_get() {
    let cache = create_test_cache();
    let key = "test_key";
    let value = b"test_value";

    cache.set(key, value, None).unwrap();
    let result = cache.get(key).unwrap();

    assert_eq!(result, Some(value.to_vec()));
}

#[test]
fn test_delete() {
    let cache = create_test_cache();
    let key = "test_key";
    let value = b"test_value";

    cache.set(key, value, None).unwrap();
    cache.delete(key).unwrap();
    let result = cache.get(key).unwrap();

    assert_eq!(result, None);
}

#[test]
fn test_expiration() {
    let cache = create_test_cache();
    let key = "test_key";
    let value = b"test_value";

    // Set with 1 second TTL
    cache.set(key, value, Some(Duration::from_secs(1))).unwrap();

    // Should be available immediately
    let result = cache.get(key).unwrap();
    assert_eq!(result, Some(value.to_vec()));

    // Wait for expiration
    thread::sleep(Duration::from_secs(2));

    // Call get multiple times to ensure the expiration check is triggered
    // The first call might not trigger the check if the value is still in the memory cache
    for _ in 0..3 {
        let result = cache.get(key).unwrap();
        if result.is_none() {
            // Test passes if we get None
            return;
        }
        // Wait a bit before trying again
        thread::sleep(Duration::from_millis(500));
    }

    // If we get here, the test fails
    panic!("Value did not expire after multiple attempts");
}
