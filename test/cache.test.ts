import assert from 'node:assert/strict';
import test from 'node:test';
import { ArtifactCache, MemoryBackend, cacheKey, encodeArtifact, type CacheIdentity } from '../src/index.js';

const identity: CacheIdentity = { project: 'p', owner: 'samples', format: { producer: 'samples', version: 1 }, revision: 'r1' };
const id = (owner: string): CacheIdentity => ({ ...identity, owner });

test('identities isolate project, owner, revision and producer/version without separator collisions', async () => {
  const first = await cacheKey(identity);
  for (const change of [ { project: 'q' }, { owner: 'other' }, { revision: 'r2' },
    { format: { producer: 'other', version: 1 } }, { format: { producer: 'samples', version: 2 } } ]) {
    assert.notEqual(await cacheKey({ ...identity, ...change }), first);
  }
  assert.notEqual(await cacheKey({ ...identity, owner: 'a|b', revision: 'c' }), await cacheKey({ ...identity, owner: 'a', revision: 'b|c' }));
});

test('cache persists across instances and put snapshots before asynchronous hashing', async () => {
  const backend = new MemoryBackend();
  const cache = new ArtifactCache(backend);
  const value = new Float32Array([2, 3]);
  const pending = cache.put(identity, value);
  value[0] = 99;
  await pending;
  await cache.flush();
  const next = new ArtifactCache(backend);
  const result = await next.getOrCompute(identity, () => { throw new Error('must not run'); });
  assert.equal(result.cacheHit, true);
  assert.deepEqual(result.value, new Float32Array([2, 3]));
});

test('LRU eviction is byte based and replacement does not double-count an entry', async () => {
  const backend = new MemoryBackend();
  let now = 1;
  const value = new Float32Array([1, 2, 3]);
  const size = encodeArtifact(value, identity.format).length;
  const cache = new ArtifactCache(backend, { budgetBytes: size * 2, now: () => now++ });
  await cache.put(id('a'), value);
  await cache.put(id('b'), value);
  assert.deepEqual(await cache.get(id('a')), value);
  await cache.put(id('c'), value);
  assert.equal(await cache.get(id('b')), undefined);
  assert.deepEqual(await cache.get(id('a')), value);
  await cache.put(id('a'), value);
  assert.equal(cache.stats().entries, 2);
  assert.equal(cache.stats().storedBytes, 2 * size);
});

test('queued-byte limit includes writes waiting for a backend and flush waits for them', async () => {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  class Slow extends MemoryBackend {
    override async write(key: string, bytes: Uint8Array, time: number) { await gate; await super.write(key, bytes, time); }
  }
  const value = new Float32Array(100);
  const size = encodeArtifact(value, identity.format).length;
  const cache = new ArtifactCache(new Slow(), { maxQueuedBytes: size });
  const first = cache.put(id('a'), value);
  await cache.put(id('b'), value);
  assert.equal(cache.stats().dropped, 1);
  assert.equal(cache.stats().queuedBytes, size);
  let flushed = false;
  const flush = cache.flush().then(() => { flushed = true; });
  await new Promise((resolve) => setTimeout(resolve, 5));
  assert.equal(flushed, false);
  release(); await first; await flush;
  assert.equal(cache.stats().queuedBytes, 0);
  assert.equal(cache.stats().peakQueuedBytes, size);
});

test('failed writes and hashes release reservations and do not poison subsequent writes', async () => {
  class Flaky extends MemoryBackend {
    fail = true;
    override async write(key: string, bytes: Uint8Array, time: number) {
      if (this.fail) { this.fail = false; throw new Error('full disk'); }
      await super.write(key, bytes, time);
    }
  }
  const cache = new ArtifactCache(new Flaky(), { onError: () => { throw new Error('bad logger'); } });
  await cache.put(identity, 1);
  await cache.put({ ...identity, project: '' }, 2);
  await cache.put(identity, 3);
  assert.equal(await cache.get(identity), 3);
  assert.equal(cache.stats().queuedBytes, 0);
  assert.equal(cache.stats().errors, 2);
});

test('corrupt or wrong-version entries recompute, and producer exceptions propagate', async () => {
  const backend = new MemoryBackend();
  await backend.write(await cacheKey(identity), new Uint8Array([1, 2, 3]), 0);
  const cache = new ArtifactCache(backend);
  const result = await cache.getOrCompute(identity, () => 42);
  assert.deepEqual(result, { value: 42, cacheHit: false });
  await cache.flush();
  assert.equal(await cache.get(identity), 42);
  await assert.rejects(cache.getOrCompute(id('broken'), () => { throw new Error('producer'); }), /producer/);
  assert.equal((await cache.getOrCompute(id('broken'), () => 7)).value, 7);
});

test('oversized entries are not persisted, but computations still complete', async () => {
  const cache = new ArtifactCache(new MemoryBackend(), { budgetBytes: 0 });
  assert.equal((await cache.getOrCompute(identity, () => 9)).value, 9);
  await cache.flush();
  assert.equal(cache.stats().storedBytes, 0);
  assert.equal(cache.stats().dropped, 1);
});
