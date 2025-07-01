const { JsCache } = require('../');

async function main() {
  // Create a new cache instance
  const cache = new JsCache({
    maxMemoryMb: 100,
    dbPath: 'tests/db/example-cache.sqlite',
  });

  // Set a value
  const key = 'example-key';
  const value = Buffer.from('Hello, world!');
  await cache.set(key, value);
  console.log(`Set value for key: ${key}`);

  // Set a value with TTL (5 seconds)
  const ttlKey = 'ttl-example';
  const ttlValue = Buffer.from('This will expire in 5 seconds');
  await cache.set(ttlKey, ttlValue, 5000);
  console.log(`Set value with 5 second TTL for key: ${ttlKey}`);

  // Get a value
  const retrievedValue = await cache.get(key);
  if (retrievedValue) {
    console.log(`Retrieved value for key ${key}: ${retrievedValue.toString()}`);
  } else {
    console.log(`No value found for key: ${key}`);
  }

  // Wait 6 seconds and try to get the TTL value
  console.log('Waiting 6 seconds for TTL value to expire...');
  await new Promise(resolve => setTimeout(resolve, 6000));

  const expiredValue = await cache.get(ttlKey);
  if (expiredValue) {
    console.log(`Retrieved value for key ${ttlKey}: ${expiredValue.toString()}`);
  } else {
    console.log(`Value for key ${ttlKey} has expired as expected`);
  }

  // Delete a value
  await cache.delete(key);
  console.log(`Deleted key: ${key}`);

  // Verify deletion
  const deletedValue = await cache.get(key);
  if (deletedValue) {
    console.log(`Retrieved value for key ${key}: ${deletedValue.toString()}`);
  } else {
    console.log(`No value found for key ${key} after deletion as expected`);
  }
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
