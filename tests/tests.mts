import test from 'ava';
import * as fs from 'fs';
import * as path from 'path';
import {JsCache} from "../napi-build/index.js";

const JS_TEST_DB_PATH = path.join(process.cwd(), 'tests', 'db', 'js_integration_tests.sqlite');

async function cleanDbFiles() {
  console.log('\nCleaning up database files before tests');

  try {
    await fs.promises.mkdir(path.dirname(JS_TEST_DB_PATH), { recursive: true });
  } catch {}

  for (const suffix of ['', '-wal', '-shm']) {
    try {
      await fs.promises.unlink(`${JS_TEST_DB_PATH}${suffix}`);
      console.log(`Deleted: ${JS_TEST_DB_PATH}${suffix}`);
    } catch (err) {
      const any_err = err as any;
      if (any_err?.code !== 'ENOENT') console.log(`Warning: Failed to delete ${JS_TEST_DB_PATH}${suffix}: ${any_err.message}`);
    }
  }

  console.log('Database cleanup completed');
}

function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}


test.before(async () => {
  await cleanDbFiles();
});

test('JsCache - set and get', async (t) => {
  const cache = await await JsCache.create({
    maxMemoryMb: 10,
    dbPath: JS_TEST_DB_PATH,
  });
  const key = 'js_test_set_get';
  const value = Buffer.from('test_value');

  await cache.set(key, value);
  const result = await cache.get(key);

  t.true(Buffer.isBuffer(result));
  t.deepEqual(result, value);
});

test('JsCache - delete', async (t) => {
  const cache = await JsCache.create({
    maxMemoryMb: 10,
    dbPath: JS_TEST_DB_PATH,
  });
  const key = 'js_test_delete';
  const value = Buffer.from('test_value');

  await cache.set(key, value);
  await cache.delete(key);
  const result = await cache.get(key);

  t.is(result, null);
});

test('JsCache - expiration', async (t) => {
  const cache = await JsCache.create({
    maxMemoryMb: 10,
    dbPath: JS_TEST_DB_PATH,
  });
  const key = 'js_test_expiration';
  const value = Buffer.from('test_value');

  await cache.set(key, value, 2000);
  const immediateResult = await cache.get(key);
  t.deepEqual(immediateResult, value);

  await new Promise(resolve => setTimeout(resolve, 4000));
  const expiredResult = await cache.get(key);

  t.is(expiredResult, null);
});

test('JsCache - memory cache TTL eviction', async (t) => {
  const cache = await JsCache.create({
    maxMemoryMb: 10,
    dbPath: JS_TEST_DB_PATH,
  });
  const key = 'js_test_ttl_eviction';
  const value = Buffer.from('memory_value');

  await cache.set(key, value, 1000);
  const immediateResult = await cache.get(key);
  t.deepEqual(immediateResult, value);

  await new Promise(resolve => setTimeout(resolve, 2000));
  const afterExpiry = await cache.get(key);
  t.is(afterExpiry, null);
});

test('JsCache - memory pressure eviction', async (t) => {
  const cache = await await JsCache.create({
    maxMemoryMb: 10,
    dbPath: JS_TEST_DB_PATH,
  });
  const firstKey = 'js_test_memory_pressure_first';
  const firstValue = Buffer.alloc(100000).fill(1);

  await cache.set(firstKey, firstValue);
  const verifyFirst = await cache.get(firstKey);
  t.deepEqual(verifyFirst, firstValue);

  for (let i = 0; i < 20; i++) {
    const key = `js_test_pressure_key_${i}`;
    const value = Buffer.alloc(100000).fill(i);
    await cache.set(key, value);
  }

  const finalCheck = await cache.get(firstKey);
  t.true(Buffer.isBuffer(finalCheck));
  t.deepEqual(finalCheck, firstValue);
});

test('JsCache - cross layer synchronization', async (t) => {
  const cache1 = await JsCache.create({
    maxMemoryMb: 10,
    dbPath: JS_TEST_DB_PATH,
  });
  const cache2 = await JsCache.create({
    maxMemoryMb: 10,
    dbPath: JS_TEST_DB_PATH,
  });

  const key = 'js_test_cross_layer';
  const value1 = Buffer.from('value1');

  await cache1.set(key, value1);
  const result2 = await cache2.get(key);
  t.deepEqual(result2, value1);

  await cache1.delete(key);
  // first access should return data, second access should return null due to lazy reconciliation
  await cache2.get(key);
  await sleep(500);

  const afterDelete = await cache2.get(key);
  t.is(afterDelete, null);
});

test('JsCache - simulated concurrent access', async (t) => {
  const numClients = 3;
  const opsPerClient = 10;

  const caches = [];
  for (let i = 0; i < numClients; i++) {
    caches.push(await JsCache.create({
      maxMemoryMb: 10,
      dbPath: JS_TEST_DB_PATH,
    }));
  }

  for (let opId = 0; opId < opsPerClient; opId++) {
    for (let clientId = 0; clientId < numClients; clientId++) {
      const key = `js_concurrent_key_${clientId}_${opId}`;
      const value = Buffer.from(`value_${clientId}_${opId}`);

      await caches[clientId].set(key, value);
      const result = await caches[clientId].get(key);
      t.deepEqual(result, value);

      if (opId % 3 === 0) {
        await caches[clientId].delete(key);
        const deletedResult = await caches[clientId].get(key);
        t.is(deletedResult, null);
      }
    }
  }
});

test('JsCache - memory limit enforcement', async (t) => {
  const cache = await JsCache.create({
    maxMemoryMb: 10,
    dbPath: JS_TEST_DB_PATH,
  });

  for (let i = 0; i < 10; i++) {
    const key = `js_memory_limit_key_${i}`;
    const value = Buffer.alloc(10000).fill(i);
    await cache.set(key, value);
  }

  const largeKey = 'js_memory_limit_large';
  const largeValue = Buffer.alloc(500000);
  await cache.set(largeKey, largeValue);

  const result = await cache.get(largeKey);
  t.deepEqual(result, largeValue);
});
