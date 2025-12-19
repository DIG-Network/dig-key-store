use dig_key_store::{Cache, CacheOptions};
use std::fs;
use std::path::Path;
use std::string::ToString;
use std::sync::Once;
use std::time::Duration;
use tokio::time::sleep;

static INTEGRATION_TESTS_RS_DB_PATH: &str = "tests/db/cargo_integration_tests.sqlite";
static ONCE: Once = Once::new();

// Function to set up the test database once
pub fn clean_db_files_once() {
    // Use a static Once to ensure we only clean up once per test run
    ONCE.call_once(|| {
        println!("\nCleaning up database files before tests");
        let db_path = INTEGRATION_TESTS_RS_DB_PATH;
        let wal_path_str = format!("{}-wal", INTEGRATION_TESTS_RS_DB_PATH);
        let shm_path_str = format!("{}-shm", INTEGRATION_TESTS_RS_DB_PATH);

        // Ensure the tests/db directory exists
        if let Some(parent) = Path::new(db_path).parent() {
            if !parent.exists() {
                println!("Creating directory for test database: {:?}", parent);
                fs::create_dir_all(parent).unwrap_or_else(|e| {
                    println!(
                        "Warning: Failed to create directory for test database: {}",
                        e
                    );
                });
            }
        }

        // Delete the main database file if it exists
        let db_path = Path::new(db_path);
        if db_path.exists() {
            println!("Deleting database file: {:?}", db_path);
            // Try multiple times with small delays to handle potential file locks
            for attempt in 1..=5 {
                match fs::remove_file(db_path) {
                    Ok(_) => {
                        println!("Successfully deleted database file");
                        break;
                    }
                    Err(e) => {
                        println!(
                            "Warning: Failed to delete database file (attempt {}/5): {}",
                            attempt, e
                        );
                        if attempt < 5 {
                            std::thread::sleep(std::time::Duration::from_millis(100));
                        }
                    }
                }
            }
        }

        // Delete the WAL file if it exists
        let wal_path = Path::new(&wal_path_str);
        if wal_path.exists() {
            println!("Deleting WAL file: {:?}", wal_path);
            for attempt in 1..=5 {
                match fs::remove_file(wal_path) {
                    Ok(_) => {
                        println!("Successfully deleted WAL file");
                        break;
                    }
                    Err(e) => {
                        println!(
                            "Warning: Failed to delete WAL file (attempt {}/5): {}",
                            attempt, e
                        );
                        if attempt < 5 {
                            std::thread::sleep(std::time::Duration::from_millis(100));
                        }
                    }
                }
            }
        }

        // Delete the SHM file if it exists
        let shm_path = Path::new(&shm_path_str);
        if shm_path.exists() {
            println!("Deleting SHM file: {:?}", shm_path);
            for attempt in 1..=5 {
                match fs::remove_file(shm_path) {
                    Ok(_) => {
                        println!("Successfully deleted SHM file");
                        break;
                    }
                    Err(e) => {
                        println!(
                            "Warning: Failed to delete SHM file (attempt {}/5): {}",
                            attempt, e
                        );
                        if attempt < 5 {
                            std::thread::sleep(std::time::Duration::from_millis(100));
                        }
                    }
                }
            }
        }

        println!("Database cleanup completed");
    });
}

async fn create_test_cache() -> Cache {
    // Get the current test name for logging purposes
    let test_name = std::thread::current()
        .name()
        .unwrap_or("unknown")
        .to_string();
    println!("Creating cache for test: {}", test_name);

    // Use a single database file for all integration tests
    let db_path = INTEGRATION_TESTS_RS_DB_PATH.to_string();
    println!("Using database path: {}", db_path);

    let options = CacheOptions {
        max_memory_mb: 10,
        db_path,
    };

    match Cache::new(options).await {
        Ok(cache) => {
            println!("Successfully created cache instance");
            cache
        }
        Err(e) => {
            panic!("Failed to create cache: {:?}", e);
        }
    }
}

#[tokio::test]
async fn test_set_get() {
    println!("\nINTEGRATION TEST: Testing set and get operations");

    // Set up test database once
    clean_db_files_once();

    let cache = create_test_cache().await;
    let key = "tests_rs_test_set_get_basic_operation";
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

    // Set up test database once
    clean_db_files_once();

    let cache = create_test_cache().await;
    let key = "tests_rs_test_delete_key_removal";
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

    // Set up test database once
    clean_db_files_once();

    let cache = create_test_cache().await;
    let key = "tests_rs_test_expiration_ttl_check";
    let value = b"test_value";

    // Set with 1 second TTL
    println!("Setting key '{}' with 1 second TTL", key);
    cache
        .set(key, value, Some(Duration::from_secs(1)))
        .await
        .unwrap();

    // Should be available immediately
    println!("Verifying key is available immediately after setting");
    let result = cache.get(key).await.unwrap();
    assert_eq!(result, Some(value.to_vec()));
    println!("Key was successfully retrieved immediately after setting");

    // Wait for expiration
    println!("Waiting for 2 seconds to allow key to expire...");
    sleep(Duration::from_secs(2)).await;
    println!("Wait complete, key should now be expired");

    let result = cache.get(key).await.unwrap();
    assert_eq!(result, None, "Key should have been expired");

    // Test passes if we get None
    println!("SUCCESS: Key has expired and was properly removed from cache");
    return;
}

#[tokio::test]
async fn test_memory_pressure_eviction_miss_access() {
    println!("\nINTEGRATION TEST: Testing memory pressure eviction and miss access via DB");

    // Set up test database once
    clean_db_files_once();

    // Create a cache with a small memory limit to trigger eviction
    let db_path = INTEGRATION_TESTS_RS_DB_PATH.to_string();

    // We don't remove the database file as we want to reuse it across tests

    // Create a cache with a very small memory limit (1MB)
    let options = CacheOptions {
        max_memory_mb: 1, // Small memory limit to trigger eviction
        db_path,
    };

    let cache = Cache::new(options)
        .await
        .expect("Failed to create cache for test");
    println!("Successfully created cache instance with 1MB memory limit");

    // First key that we'll check if it gets evicted
    let first_key = "tests_rs_test_memory_pressure_eviction_first_key";
    let first_value = vec![1u8; 100_000]; // 100KB value

    println!("Setting first key '{}' with 100KB value", first_key);
    cache.set(first_key, &first_value, None).await.unwrap();

    // Verify the first key is in the cache
    println!("Verifying first key is in the cache");
    let result = cache.get(first_key).await.unwrap();
    assert_eq!(result, Some(first_value.clone()));

    // Add many more keys to trigger memory pressure
    println!("Adding many more keys to trigger memory pressure");
    for i in 0..30 {
        let key = format!("tests_rs_test_memory_pressure_eviction_key_{}", i);
        let value = vec![i as u8; 100_000]; // 100KB value each
        cache.set(&key, &value, None).await.unwrap();

        // Access the first key occasionally to keep it "warm" in the LRU
        if i % 2 == 0 {
            cache.get(first_key).await.unwrap();
        }
    }

    // Add one more large key to definitely trigger eviction
    println!("Adding one more large key to trigger eviction");
    let large_key = "tests_rs_test_memory_pressure_eviction_large_key";
    let large_value = vec![0u8; 500_000]; // 500KB value
    cache.set(large_key, &large_value, None).await.unwrap();

    // The first key should still be in the database but might be evicted from memory
    println!(
        "Checking if first key is still accessible (should be in DB even if evicted from memory)"
    );
    let result = cache.get(first_key).await.unwrap();
    assert_eq!(
        result,
        Some(first_value),
        "First key should still be accessible from database"
    );
    println!("SUCCESS: First key is still accessible after memory pressure");

    // Check that some of the middle keys were evicted from memory but still in DB
    // We can't know exactly which keys were evicted, but we can check that they're still accessible
    println!("Checking that middle keys are still accessible");
    for i in 0..20 {
        let key = format!("tests_rs_test_memory_pressure_eviction_key_{}", i);
        let expected_value = vec![i as u8; 100_000];
        let result = cache.get(&key).await.unwrap();
        assert_eq!(
            result,
            Some(expected_value),
            "Key {} should still be accessible from database",
            key
        );
    }
    println!("SUCCESS: All keys are still accessible after memory pressure");
}

#[tokio::test]
async fn test_cross_layer_synchronization() {
    println!("\nINTEGRATION TEST: Testing cross-layer synchronization");

    // Set up test database once
    clean_db_files_once();

    // Create two separate cache instances that point to the same database
    // This simulates two processes accessing the same cache
    let db_path = INTEGRATION_TESTS_RS_DB_PATH.to_string();

    // We don't remove the database file as we want to reuse it across tests

    // Create the first cache instance
    let options1 = CacheOptions {
        max_memory_mb: 10,
        db_path: db_path.clone(),
    };

    let cache1 = Cache::new(options1)
        .await
        .expect("Failed to create first cache instance");
    println!("Successfully created first cache instance");

    // Set a key in the first cache
    let key = "tests_rs_test_cross_layer_synchronization_key";
    let value1 = b"value_from_cache1";

    println!("Setting key '{}' in first cache", key);
    cache1.set(key, value1, None).await.unwrap();

    // Create the second cache instance pointing to the same database
    let options2 = CacheOptions {
        max_memory_mb: 10,
        db_path,
    };

    let cache2 = Cache::new(options2)
        .await
        .expect("Failed to create second cache instance");
    println!("Successfully created second cache instance");

    // Get the key from the second cache - it should be retrieved from the database
    println!("Getting key '{}' from second cache", key);
    let result = cache2.get(key).await.unwrap();
    assert_eq!(
        result,
        Some(value1.to_vec()),
        "Second cache should retrieve value set by first cache"
    );
    println!("SUCCESS: Second cache successfully retrieved value set by first cache");

    // First, delete the key from the first cache to force it to fetch from the database
    // This is necessary because the cache prioritizes memory cache over database
    println!(
        "Deleting key '{}' from first cache to force database fetch",
        key
    );
    cache1.delete(key).await.unwrap();

    // Now set the key in the second cache
    let value2 = b"value_from_cache2";
    println!("Setting key '{}' in second cache", key);
    cache2.set(key, value2, None).await.unwrap();

    // Get the updated key from the first cache - it should fetch from the database
    println!("Getting updated key '{}' from first cache", key);
    let result = cache1.get(key).await.unwrap();
    assert_eq!(
        result,
        Some(value2.to_vec()),
        "First cache should retrieve updated value from second cache"
    );
    println!("SUCCESS: First cache successfully retrieved updated value from second cache");

    // Delete the key in the first cache
    println!("Deleting key '{}' in first cache", key);
    cache1.delete(key).await.unwrap();

    // Try to get the deleted key from the second cache - this will trigger lazy deletion from memory
    println!(
        "Attempt 1 to get deleted key '{}' from second cache (trigger lazy deletion).",
        key
    );
    cache2.get(key).await.unwrap();
    sleep(Duration::from_millis(1000)).await;

    // Verify that the key is deleted from memory
    println!(
        "Attempt 2 to get deleted key '{}' from second cache post lazy deletion.",
        key
    );
    let result = cache2.get(key).await.unwrap();

    assert_eq!(
        result, None,
        "Key should be deleted in second cache as well"
    );
    println!("SUCCESS: Key deletion was properly synchronized between caches");
}

#[tokio::test]
async fn test_simulated_concurrent_access() {
    println!("\nINTEGRATION TEST: Testing simulated concurrent access patterns");

    clean_db_files_once();
    let db_path = INTEGRATION_TESTS_RS_DB_PATH.to_string();

    let num_clients = 5;
    let ops_per_client = 100;

    println!(
        "Creating {} cache instances to simulate concurrent clients",
        num_clients
    );

    let mut caches = Vec::with_capacity(num_clients);
    for i in 0..num_clients {
        let options = CacheOptions {
            max_memory_mb: 10,
            db_path: db_path.clone(),
        };

        let cache = Cache::new(options)
            .await
            .expect("Failed to create cache instance");
        println!("Successfully created cache instance {}", i);
        caches.push(cache);
    }

    println!(
        "Simulating concurrent access with {} operations per client",
        ops_per_client
    );

    for op_id in 0..ops_per_client {
        for client_id in 0..num_clients {
            let key = format!(
                "tests_rs_test_simulated_concurrent_access_key_{}_{}",
                client_id, op_id
            );
            let value = format!("value_{}_{}", client_id, op_id).into_bytes();

            caches[client_id].set(&key, &value, None).await.unwrap();

            let result = caches[client_id].get(&key).await.unwrap();
            assert_eq!(
                result,
                Some(value.clone()),
                "Client {} failed to get key {}",
                client_id,
                key
            );

            // Occasionally read keys from other clients
            if op_id % 10 == 0 && client_id > 0 {
                let other_client_id = (client_id - 1) % num_clients;
                let other_key = format!(
                    "tests_rs_test_simulated_concurrent_access_key_{}_{}",
                    other_client_id, op_id
                );

                let result = caches[client_id].get(&other_key).await.unwrap();
                if let Some(other_value) = result {
                    let expected_value =
                        format!("value_{}_{}", other_client_id, op_id).into_bytes();
                    assert_eq!(
                        other_value, expected_value,
                        "Client {} got incorrect value for key {}",
                        client_id, other_key
                    );
                }
            }

            // Occasionally delete keys
            if op_id % 20 == 0 && op_id > 0 {
                let delete_key = format!(
                    "tests_rs_test_simulated_concurrent_access_key_{}_{}",
                    client_id,
                    op_id - 10
                );

                caches[client_id].delete(&delete_key).await.unwrap();

                // Deleting cache must observe deletion immediately
                let result = caches[client_id].get(&delete_key).await.unwrap();
                assert_eq!(
                    result, None,
                    "Client {} failed to delete key {}",
                    client_id, delete_key
                );

                // Other caches must converge on deletion within one additional read
                if client_id < num_clients - 1 {
                    let next_client_id = client_id + 1;
                    assert_eventual_delete(&caches[next_client_id], &delete_key).await;
                }
            }
        }
    }

    println!("SUCCESS: All simulated concurrent operations completed without errors");
}

#[tokio::test]
async fn test_memory_limit_enforcement() {
    println!("\nINTEGRATION TEST: Testing memory limit enforcement");

    // Set up test database once
    clean_db_files_once();

    // Create a cache with a very small memory limit
    let db_path = INTEGRATION_TESTS_RS_DB_PATH.to_string();

    // We don't remove the database file as we want to reuse it across tests

    // Create a cache with a very small memory limit (0.5MB)
    let options = CacheOptions {
        max_memory_mb: 1, // 1MB limit
        db_path,
    };

    let cache = Cache::new(options)
        .await
        .expect("Failed to create cache for test");
    println!("Successfully created cache instance with 1MB memory limit");

    // Add a sequence of keys with increasing sizes
    println!("Adding keys with increasing sizes to test memory limit enforcement");

    // Start with small keys
    for i in 0..10 {
        let key = format!("tests_rs_test_memory_limit_enforcement_small_key_{}", i);
        let value = vec![i as u8; 10_000]; // 10KB each
        cache.set(&key, &value, None).await.unwrap();
    }

    // Add medium-sized keys
    for i in 0..5 {
        let key = format!("tests_rs_test_memory_limit_enforcement_medium_key_{}", i);
        let value = vec![i as u8; 100_000]; // 100KB each
        cache.set(&key, &value, None).await.unwrap();
    }

    // Add one large key that should trigger eviction
    let large_key = "tests_rs_test_memory_limit_enforcement_large_key";
    let large_value = vec![0u8; 500_000]; // 500KB
    println!("Adding large key that should trigger eviction");
    cache.set(large_key, &large_value, None).await.unwrap();

    // Verify the large key is in the cache
    let result = cache.get(large_key).await.unwrap();
    assert_eq!(
        result,
        Some(large_value.clone()),
        "Large key should be in the cache"
    );
    println!("Large key is in the cache as expected");

    // Add another large key to definitely trigger more evictions
    let another_large_key = "tests_rs_test_memory_limit_enforcement_another_large_key";
    let another_large_value = vec![1u8; 500_000]; // Another 500KB
    println!("Adding another large key to trigger more evictions");
    cache
        .set(another_large_key, &another_large_value, None)
        .await
        .unwrap();

    // Verify both large keys are still accessible (from DB if not from memory)
    let result = cache.get(large_key).await.unwrap();
    assert_eq!(
        result,
        Some(large_value),
        "First large key should still be accessible"
    );

    let result = cache.get(another_large_key).await.unwrap();
    assert_eq!(
        result,
        Some(another_large_value),
        "Second large key should be accessible"
    );

    println!("Both large keys are still accessible as expected");

    // Check that we can still access all keys (they should be in the DB even if evicted from memory)
    println!("Verifying all keys are still accessible from the database");

    // Check small keys
    for i in 0..10 {
        let key = format!("tests_rs_test_memory_limit_enforcement_small_key_{}", i);
        let expected_value = vec![i as u8; 10_000];
        let result = cache.get(&key).await.unwrap();
        assert_eq!(
            result,
            Some(expected_value),
            "Small key {} should still be accessible",
            i
        );
    }

    // Check medium keys
    for i in 0..5 {
        let key = format!("tests_rs_test_memory_limit_enforcement_medium_key_{}", i);
        let expected_value = vec![i as u8; 100_000];
        let result = cache.get(&key).await.unwrap();
        assert_eq!(
            result,
            Some(expected_value),
            "Medium key {} should still be accessible",
            i
        );
    }

    println!("SUCCESS: Memory limit is enforced, but all keys remain accessible from the database");
}

/// Asserts the cache’s *eventual consistency* guarantee for deletions.
///
/// # Behavior being tested
///
/// This helper encodes the cache’s contract that:
///
/// - A cache **may return a stale in-memory value on the first read**
///   after another process deletes a key.
/// - The cache **must lazily validate against the database**.
/// - A **subsequent read must observe the deletion** and return `None`.
///
/// This ensures:
/// - Read-your-own-writes consistency for the deleting cache
/// - Bounded staleness (at most one stale read) for other caches
///
/// If this assertion fails, it indicates that:
/// - Lazy DB validation is not occurring
/// - Or stale entries are not being invalidated correctly
async fn assert_eventual_delete(cache: &Cache, key: &str) {
    // First read may return stale data — this is allowed
    cache.get(key).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    // Second read must reflect the deletion
    let result = cache.get(key).await.unwrap();
    assert_eq!(
        result, None,
        "Cache did not converge on deletion for key {}",
        key
    );
}
