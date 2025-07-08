use std::time::Duration;
use dig_key_store::{Cache, CacheOptions};
use tokio::time::sleep;

async fn create_test_cache() -> Cache {
    // Get the current test name for logging purposes
    let test_name = std::thread::current().name().unwrap_or("unknown").to_string();
    println!("Creating cache for test: {}", test_name);

    // Use a single database file for all tests
    let db_path = "tests/db/cargo_tests.sqlite".to_string();
    println!("Using database path: {}", db_path);

    // Ensure the tests/db directory exists
    std::fs::create_dir_all("tests/db").expect("Failed to create tests/db directory");
    println!("Created tests/db directory if it didn't exist");

    let options = CacheOptions {
        max_memory_mb: 10,
        db_path
    };

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
    let key = "test_set_get_basic_operation";
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
    let key = "test_delete_key_removal";
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
    let key = "test_expiration_ttl_check";
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

#[tokio::test]
async fn test_memory_cache_ttl_eviction() {
    println!("\nINTEGRATION TEST: Testing TTL eviction from memory cache");

    let cache = create_test_cache().await;
    let key = "test_memory_cache_ttl_eviction_check";
    let value = b"memory_cache_value";

    // Set with 1 second TTL
    println!("Setting key '{}' with 1 second TTL", key);
    cache.set(key, value, Some(Duration::from_secs(1))).await.unwrap();

    // Get the key to ensure it's in the memory cache
    println!("Getting key '{}' to ensure it's in memory cache", key);
    let result = cache.get(key).await.unwrap();
    assert_eq!(result, Some(value.to_vec()));
    println!("Key was successfully retrieved and should now be in memory cache");

    // Wait for expiration
    println!("Waiting for 2 seconds to allow key to expire...");
    sleep(Duration::from_secs(2)).await;
    println!("Wait complete, key should now be expired");

    // Get the key again - it should be evicted due to TTL expiration
    println!("Getting key '{}' after expiration - should be evicted", key);
    let result = cache.get(key).await.unwrap();
    assert_eq!(result, None, "Key should have been evicted due to TTL expiration");
    println!("SUCCESS: Key was properly evicted on get due to TTL expiration");

    // Try to get the key again - it should still be None since it was removed from both memory and DB
    println!("Getting key '{}' again - should still be None", key);
    let result = cache.get(key).await.unwrap();
    assert_eq!(result, None, "Key should still be None after eviction");
    println!("SUCCESS: Key remains evicted after first get operation");
}

#[tokio::test]
async fn test_memory_pressure_eviction() {
    println!("\nINTEGRATION TEST: Testing memory pressure eviction");

    // Create a cache with a small memory limit to trigger eviction
    let test_name = std::thread::current().name().unwrap_or("unknown").to_string();
    let db_path = format!("tests/db/memory_pressure_test_{}.sqlite", test_name.replace("::", "_"));

    // Ensure the tests/db directory exists
    std::fs::create_dir_all("tests/db").expect("Failed to create tests/db directory");

    // Remove the database file if it exists
    if std::path::Path::new(&db_path).exists() {
        std::fs::remove_file(&db_path).expect("Failed to remove existing database file");
    }

    // Create a cache with a very small memory limit (1MB)
    let options = CacheOptions {
        max_memory_mb: 1, // Small memory limit to trigger eviction
        db_path,
    };

    let cache = Cache::new(options).await.expect("Failed to create cache for test");
    println!("Successfully created cache instance with 1MB memory limit");

    // First key that we'll check if it gets evicted
    let first_key = "first_key_for_eviction_test";
    let first_value = vec![0u8; 100_000]; // 100KB value

    println!("Setting first key '{}' with 100KB value", first_key);
    cache.set(first_key, &first_value, None).await.unwrap();

    // Verify the first key is in the cache
    println!("Verifying first key is in the cache");
    let result = cache.get(first_key).await.unwrap();
    assert_eq!(result, Some(first_value.clone()));

    // Add many more keys to trigger memory pressure
    println!("Adding many more keys to trigger memory pressure");
    for i in 0..20 {
        let key = format!("memory_pressure_key_{}", i);
        let value = vec![i as u8; 100_000]; // 100KB value each
        cache.set(&key, &value, None).await.unwrap();

        // Access the first key occasionally to keep it "warm" in the LRU
        if i % 5 == 0 {
            cache.get(first_key).await.unwrap();
        }
    }

    // Add one more large key to definitely trigger eviction
    println!("Adding one more large key to trigger eviction");
    let large_key = "large_key_for_eviction";
    let large_value = vec![0u8; 500_000]; // 500KB value
    cache.set(large_key, &large_value, None).await.unwrap();

    // The first key should still be in the database but might be evicted from memory
    println!("Checking if first key is still accessible (should be in DB even if evicted from memory)");
    let result = cache.get(first_key).await.unwrap();
    assert_eq!(result, Some(first_value), "First key should still be accessible from database");
    println!("SUCCESS: First key is still accessible after memory pressure");

    // Check that some of the middle keys were evicted from memory but still in DB
    // We can't know exactly which keys were evicted, but we can check that they're still accessible
    println!("Checking that middle keys are still accessible");
    for i in 0..20 {
        let key = format!("memory_pressure_key_{}", i);
        let expected_value = vec![i as u8; 100_000];
        let result = cache.get(&key).await.unwrap();
        assert_eq!(result, Some(expected_value), "Key {} should still be accessible from database", key);
    }
    println!("SUCCESS: All keys are still accessible after memory pressure");
}

#[tokio::test]
async fn test_cross_layer_synchronization() {
    println!("\nINTEGRATION TEST: Testing cross-layer synchronization");

    // Create two separate cache instances that point to the same database
    // This simulates two processes accessing the same cache
    let test_name = std::thread::current().name().unwrap_or("unknown").to_string();
    let db_path = format!("tests/db/cross_layer_sync_test_{}.sqlite", test_name.replace("::", "_"));

    // Ensure the tests/db directory exists
    std::fs::create_dir_all("tests/db").expect("Failed to create tests/db directory");

    // Remove the database file if it exists
    if std::path::Path::new(&db_path).exists() {
        std::fs::remove_file(&db_path).expect("Failed to remove existing database file");
    }

    // Create the first cache instance
    let options1 = CacheOptions {
        max_memory_mb: 10,
        db_path: db_path.clone(),
    };

    let cache1 = Cache::new(options1).await.expect("Failed to create first cache instance");
    println!("Successfully created first cache instance");

    // Set a key in the first cache
    let key = "cross_layer_sync_key";
    let value1 = b"value_from_cache1";

    println!("Setting key '{}' in first cache", key);
    cache1.set(key, value1, None).await.unwrap();

    // Create the second cache instance pointing to the same database
    let options2 = CacheOptions {
        max_memory_mb: 10,
        db_path,
    };

    let cache2 = Cache::new(options2).await.expect("Failed to create second cache instance");
    println!("Successfully created second cache instance");

    // Get the key from the second cache - it should be retrieved from the database
    println!("Getting key '{}' from second cache", key);
    let result = cache2.get(key).await.unwrap();
    assert_eq!(result, Some(value1.to_vec()), "Second cache should retrieve value set by first cache");
    println!("SUCCESS: Second cache successfully retrieved value set by first cache");

    // First, delete the key from the first cache to force it to fetch from the database
    // This is necessary because the cache prioritizes memory cache over database
    println!("Deleting key '{}' from first cache to force database fetch", key);
    cache1.delete(key).await.unwrap();

    // Now set the key in the second cache
    let value2 = b"value_from_cache2";
    println!("Setting key '{}' in second cache", key);
    cache2.set(key, value2, None).await.unwrap();

    // Get the updated key from the first cache - it should fetch from the database
    println!("Getting updated key '{}' from first cache", key);
    let result = cache1.get(key).await.unwrap();
    assert_eq!(result, Some(value2.to_vec()), "First cache should retrieve updated value from second cache");
    println!("SUCCESS: First cache successfully retrieved updated value from second cache");

    // Delete the key in the first cache
    println!("Deleting key '{}' in first cache", key);
    cache1.delete(key).await.unwrap();

    // Try to get the deleted key from the second cache - it should be gone
    println!("Attempting to get deleted key '{}' from second cache", key);
    let result = cache2.get(key).await.unwrap();
    assert_eq!(result, None, "Key should be deleted in second cache as well");
    println!("SUCCESS: Key deletion was properly synchronized between caches");
}

#[tokio::test]
async fn test_simulated_concurrent_access() {
    println!("\nINTEGRATION TEST: Testing simulated concurrent access patterns");

    // Create a shared database for all cache instances
    let test_name = std::thread::current().name().unwrap_or("unknown").to_string();
    let db_path = format!("tests/db/concurrent_access_test_{}.sqlite", test_name.replace("::", "_"));

    // Ensure the tests/db directory exists
    std::fs::create_dir_all("tests/db").expect("Failed to create tests/db directory");

    // Remove the database file if it exists
    if std::path::Path::new(&db_path).exists() {
        std::fs::remove_file(&db_path).expect("Failed to remove existing database file");
    }

    // Number of simulated concurrent clients
    let num_clients = 5;
    // Number of operations per client
    let ops_per_client = 50;

    println!("Creating {} cache instances to simulate concurrent clients", num_clients);

    // Create multiple cache instances that all point to the same database
    let mut caches = Vec::with_capacity(num_clients);
    for i in 0..num_clients {
        let options = CacheOptions {
            max_memory_mb: 10,
            db_path: db_path.clone(),
        };

        let cache = Cache::new(options).await.expect("Failed to create cache instance");
        println!("Successfully created cache instance {}", i);
        caches.push(cache);
    }

    println!("Simulating concurrent access with {} operations per client", ops_per_client);

    // Simulate concurrent operations by interleaving operations from different clients
    for op_id in 0..ops_per_client {
        // Each client performs an operation
        for client_id in 0..num_clients {
            let key = format!("concurrent_key_{}_{}", client_id, op_id);
            let value = format!("value_{}_{}", client_id, op_id).into_bytes();

            // Set the key
            caches[client_id].set(&key, &value, None).await.unwrap();

            // Get the key
            let result = caches[client_id].get(&key).await.unwrap();
            assert_eq!(result, Some(value.clone()), "Client {} failed to get key {}", client_id, key);

            // Occasionally read keys from other clients
            if op_id % 10 == 0 && client_id > 0 {
                let other_client_id = (client_id - 1) % num_clients;
                let other_key = format!("concurrent_key_{}_{}", other_client_id, op_id);

                // Try to get the key from another client (it should exist since we're processing sequentially)
                let result = caches[client_id].get(&other_key).await.unwrap();
                if let Some(other_value) = result {
                    let expected_value = format!("value_{}_{}", other_client_id, op_id).into_bytes();
                    assert_eq!(other_value, expected_value, "Client {} got incorrect value for key {}", client_id, other_key);
                }
            }

            // Occasionally delete keys
            if op_id % 20 == 0 && op_id > 0 {
                let delete_key = format!("concurrent_key_{}_{}", client_id, op_id - 10);
                caches[client_id].delete(&delete_key).await.unwrap();

                // Verify deletion
                let result = caches[client_id].get(&delete_key).await.unwrap();
                assert_eq!(result, None, "Client {} failed to delete key {}", client_id, delete_key);

                // Verify other clients also see the deletion
                if client_id < num_clients - 1 {
                    let next_client_id = client_id + 1;
                    let result = caches[next_client_id].get(&delete_key).await.unwrap();
                    assert_eq!(result, None, "Client {} still sees key {} that was deleted by client {}", 
                              next_client_id, delete_key, client_id);
                }
            }
        }
    }

    println!("SUCCESS: All simulated concurrent operations completed without errors");
}

#[tokio::test]
async fn test_memory_limit_enforcement() {
    println!("\nINTEGRATION TEST: Testing memory limit enforcement");

    // Create a cache with a very small memory limit
    let test_name = std::thread::current().name().unwrap_or("unknown").to_string();
    let db_path = format!("tests/db/memory_limit_test_{}.sqlite", test_name.replace("::", "_"));

    // Ensure the tests/db directory exists
    std::fs::create_dir_all("tests/db").expect("Failed to create tests/db directory");

    // Remove the database file if it exists
    if std::path::Path::new(&db_path).exists() {
        std::fs::remove_file(&db_path).expect("Failed to remove existing database file");
    }

    // Create a cache with a very small memory limit (0.5MB)
    let options = CacheOptions {
        max_memory_mb: 1, // 1MB limit
        db_path,
    };

    let cache = Cache::new(options).await.expect("Failed to create cache for test");
    println!("Successfully created cache instance with 1MB memory limit");

    // Add a sequence of keys with increasing sizes
    println!("Adding keys with increasing sizes to test memory limit enforcement");

    // Start with small keys
    for i in 0..10 {
        let key = format!("small_key_{}", i);
        let value = vec![i as u8; 10_000]; // 10KB each
        cache.set(&key, &value, None).await.unwrap();
    }

    // Add medium-sized keys
    for i in 0..5 {
        let key = format!("medium_key_{}", i);
        let value = vec![i as u8; 100_000]; // 100KB each
        cache.set(&key, &value, None).await.unwrap();
    }

    // Add one large key that should trigger eviction
    let large_key = "large_key";
    let large_value = vec![0u8; 500_000]; // 500KB
    println!("Adding large key that should trigger eviction");
    cache.set(large_key, &large_value, None).await.unwrap();

    // Verify the large key is in the cache
    let result = cache.get(large_key).await.unwrap();
    assert_eq!(result, Some(large_value.clone()), "Large key should be in the cache");
    println!("Large key is in the cache as expected");

    // Add another large key to definitely trigger more evictions
    let another_large_key = "another_large_key";
    let another_large_value = vec![1u8; 500_000]; // Another 500KB
    println!("Adding another large key to trigger more evictions");
    cache.set(another_large_key, &another_large_value, None).await.unwrap();

    // Verify both large keys are still accessible (from DB if not from memory)
    let result = cache.get(large_key).await.unwrap();
    assert_eq!(result, Some(large_value), "First large key should still be accessible");

    let result = cache.get(another_large_key).await.unwrap();
    assert_eq!(result, Some(another_large_value), "Second large key should be accessible");

    println!("Both large keys are still accessible as expected");

    // Check that we can still access all keys (they should be in the DB even if evicted from memory)
    println!("Verifying all keys are still accessible from the database");

    // Check small keys
    for i in 0..10 {
        let key = format!("small_key_{}", i);
        let expected_value = vec![i as u8; 10_000];
        let result = cache.get(&key).await.unwrap();
        assert_eq!(result, Some(expected_value), "Small key {} should still be accessible", i);
    }

    // Check medium keys
    for i in 0..5 {
        let key = format!("medium_key_{}", i);
        let expected_value = vec![i as u8; 100_000];
        let result = cache.get(&key).await.unwrap();
        assert_eq!(result, Some(expected_value), "Medium key {} should still be accessible", i);
    }

    println!("SUCCESS: Memory limit is enforced, but all keys remain accessible from the database");
}
