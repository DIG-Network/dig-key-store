# Dig Key-Value Store

A high-performance key-value cache library with both in-memory and persistent storage, written in Rust with Node.js bindings.

## Features

- Dual-layer caching with in-memory LRU cache and SQLite persistent storage
- Configurable memory limits and cleanup intervals
- Optional time-to-live (TTL) for cache entries
- Asynchronous API for Node.js
- Written in Rust for high performance

## Installation

```bash
npm install dig-key-value-store
```

## Usage

```javascript
const { JsCache } = require('dig-key-value-store');

async function example() {
  // Create a new cache instance
  const cache = new JsCache({
    maxMemoryMb: 100,         // Maximum memory usage in MB
    dbPath: './cache.db',     // Path to SQLite database file
    cleanupIntervalMs: 60000  // Cleanup interval in milliseconds (1 minute)
  });

  // Set a value
  const key = 'user:123';
  const value = Buffer.from(JSON.stringify({ name: 'John', age: 30 }));
  await cache.set(key, value);

  // Set a value with TTL (time-to-live)
  await cache.set('session:456', Buffer.from('session-data'), 3600000); // 1 hour TTL

  // Get a value
  const result = await cache.get(key);
  if (result) {
    const userData = JSON.parse(result.toString());
    console.log(userData); // { name: 'John', age: 30 }
  }

  // Delete a value
  await cache.delete(key);
}

example().catch(console.error);
```

## API

### `JsCache`

#### Constructor

```javascript
new JsCache(options)
```

Creates a new cache instance.

**Parameters:**

- `options` (Object): Configuration options
  - `maxMemoryMb` (number): Maximum memory usage in megabytes
  - `dbPath` (string): Path to the SQLite database file
  - `cleanupIntervalMs` (number): Cleanup interval in milliseconds

#### Methods

##### `set(key, value, ttlMs)`

Sets a value in the cache.

**Parameters:**

- `key` (string): The key to store the value under
- `value` (Buffer): The value to store
- `ttlMs` (number, optional): Time-to-live in milliseconds

**Returns:** Promise<void>

##### `get(key)`

Gets a value from the cache.

**Parameters:**

- `key` (string): The key to retrieve

**Returns:** Promise<Buffer | null> - The value if found, or null if not found or expired

##### `delete(key)`

Deletes a value from the cache.

**Parameters:**

- `key` (string): The key to delete

**Returns:** Promise<void>

## Development

### Prerequisites

- Node.js 14+
- Rust 1.56+
- SQLite development libraries

### Setup

1. Install sqlx-cli with cargo:
```bash
cargo install sqlx-cli
```

2. Install npm dependencies:
```bash
npm install
```

### Building

```bash
npm run build
```

### Testing

```bash
npm test
```

## License

MIT
