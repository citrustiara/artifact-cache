import assert from 'node:assert/strict';
import test from 'node:test';
import { ArtifactCache, MemoryBackend, Pipeline, type Stage } from '../src/index.js';

function makeStages(): Stage[] {
  return [
    { id: 'scale', version: 1, dependencies: [], run: ({ numbers, factor }) => Float32Array.from(numbers as Float32Array, (n) => n * Number(factor)) },
    { id: 'sum', version: 1, dependencies: ['scale'], run: (_, values) => (values.scale as Float32Array).reduce((a, b) => a + b, 0) },
    { id: 'label', version: 1, dependencies: [], run: ({ label }) => label },
  ];
}

test('pipeline computes a dependency closure and restores a warm run', async () => {
  const cache = new ArtifactCache(new MemoryBackend());
  const pipeline = new Pipeline(cache, 'p', makeStages());
  const inputs = { numbers: new Float32Array([1, 2]), factor: 3, label: 'demo' };
  const first = await pipeline.run(inputs, ['sum']);
  assert.equal(first.values.sum, 9);
  assert.deepEqual(first.built, ['scale', 'sum']);
  await cache.flush();
  const second = await pipeline.run(inputs, ['sum']);
  assert.deepEqual(second.cacheHits, ['scale', 'sum']);
  assert.deepEqual(second.values, first.values);
});

test('call-time snapshots and producer isolation keep keys associated with values', async () => {
  const cache = new ArtifactCache(new MemoryBackend());
  const pipeline = new Pipeline(cache, 'p', [
    { id: 'a', version: 1, dependencies: [], run: (inputs) => {
      (inputs.numbers as Float32Array)[0] = 999;
      return new Float32Array([4]);
    } },
    { id: 'b', version: 1, dependencies: ['a'], run: (inputs, upstream) => {
      (upstream.a as Float32Array)[0] = 88;
      return (inputs.numbers as Float32Array)[0];
    } },
  ]);
  const numbers = new Float32Array([2]);
  const pending = pipeline.run({ numbers }); numbers[0] = 7;
  const result = await pending;
  assert.equal(result.values.b, 2);
  assert.deepEqual(result.values.a, new Float32Array([4]));
});

test('invalid graph definitions fail without running any producer', () => {
  const cache = new ArtifactCache(new MemoryBackend());
  const leaf = { id: 'a', version: 1, dependencies: [], run: () => 1 };
  assert.throws(() => new Pipeline(cache, 'p', [leaf, leaf]), /Duplicate/);
  assert.throws(() => new Pipeline(cache, 'p', [{ ...leaf, dependencies: ['missing'] }]), /Unknown/);
  assert.throws(() => new Pipeline(cache, 'p', [{ ...leaf, dependencies: ['a'] }]), /Cycle/);
  assert.throws(() => new Pipeline(cache, 'p', [{ ...leaf, version: 0 }]), /version/);
});

test('changed inputs or producer versions cannot return stale values', async () => {
  const cache = new ArtifactCache(new MemoryBackend());
  const stages = makeStages();
  const input = { numbers: new Float32Array([1, 2]), factor: 3, label: 'demo' };
  const pipeline = new Pipeline(cache, 'p', stages);
  await pipeline.run(input); await cache.flush();
  assert.equal((await pipeline.run({ ...input, factor: 5 })).values.sum, 15);
  stages[1] = { ...stages[1], version: 2, run: () => 123 };
  assert.equal((await new Pipeline(cache, 'p', stages).run(input)).values.sum, 123);
});
