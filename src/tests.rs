use super::*;

#[test]
fn test_cleanup_expired_entries() {
    // Create a test cache
    let test_name = std::thread::current().name().unwrap_or("unknown").to_string();
    let db_path = format!("test_cache_unit_{}.db", test_name);

    // Remove the database file if it exists
    if std::path::Path::new(&db_path).exists() {
        std::fs::remove_file(&db_path).expect("Failed to remove existing database file");
    }

    let options = CacheOptions {
        max_memory_mb: 10,
        db_path,
        cleanup_interval: Duration::from_secs(1),
    };

    let cache = Cache::new(options).unwrap();
    let key = "test_key";
    let value = b"test_value";

    // Set with 1 second TTL
    cache.set(key, value, Some(Duration::from_secs(1))).unwrap();

    // Should be available immediately
    let result = cache.get(key).unwrap();
    assert_eq!(result, Some(value.to_vec()));

    // Wait for expiration
    thread::sleep(Duration::from_secs(2));

    // Force cleanup in a block to ensure the connection is dropped
    {
        let mut conn = cache.db_pool.get().unwrap();
        Cache::cleanup_expired_entries(&mut conn, &cache.memory_cache).unwrap();
    }

    // Should be gone after cleanup
    let result = cache.get(key).unwrap();
    assert_eq!(result, None);
}
