const test = require('ava');
const { JsCache } = require('../index');
const { promises: fs } = require('fs');
const path = require('path');

// Ensure the tests/db directory exists
async function ensureDbDirExists() {
  try {
    await fs.mkdir(path.join(__dirname, 'db'), { recursive: true });
  } catch (err) {
    // Directory might already exist, ignore error
  }
}

test('JsCache - basic operations', async (t) => {
  // Use tests/db directory for database files
  const dbPath = path.join(__dirname, 'db', 'cache-test.sqlite');
  
  // Ensure the directory exists
  await ensureDbDirExists();
  
  try {
    // Create a new cache instance
    const cache = new JsCache({
      maxMemoryMb: 10,
      dbPath,
    });
    
    // Test set and get
    const key = 'test-key';
    const value = Buffer.from('test-value');
    
    await cache.set(key, value);
    const retrieved = await cache.get(key);
    
    t.true(Buffer.isBuffer(retrieved));
    t.deepEqual(retrieved, value);
    
    // Test delete
    await cache.delete(key);
    const afterDelete = await cache.get(key);
    
    t.is(afterDelete, null);
    
    // Test TTL
    await cache.set(key, value, 100); // 100ms TTL
    
    // Value should be available immediately
    const beforeExpiry = await cache.get(key);
    t.deepEqual(beforeExpiry, value);
    
    // Wait for expiry
    await new Promise(resolve => setTimeout(resolve, 200));
    
    // Value should be expired now
    const afterExpiry = await cache.get(key);
    t.is(afterExpiry, null);
    
  } finally {
    // Clean up
    try {
      await fs.unlink(dbPath);
    } catch (err) {
      // Ignore errors during cleanup
    }
  }
});