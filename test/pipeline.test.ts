import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ArtifactCache, MemoryBackend, Pipeline, type Stage } from '../src/index.js';
import { FileBackend } from '../src/backends/file.js';

function makeStages(): Stage[] {
  return [
    { id: 'scale', version: 1, dependencies: [], inputKeys: ['numbers', 'factor'], run: ({ numbers, factor }) => Float32Array.from(numbers as Float32Array, (n) => n * Number(factor)) },
    { id: 'sum', version: 1, dependencies: ['scale'], inputKeys: [], run: (_, values) => (values.scale as Float32Array).reduce((a, b) => a + b, 0) },
    { id: 'label', version: 1, dependencies: [], inputKeys: ['label'], run: ({ label }) => label },
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
  assert.throws(() => new Pipeline(cache, 'p', [{ ...leaf, inputKeys: ['a', 'a'] }]), /Duplicate/);
  assert.throws(() => new Pipeline(cache, 'p', [{ ...leaf, inputKeys: [123 as unknown as string] }]), /strings/);
  assert.throws(() => new Pipeline(cache, 'p', [{ ...leaf, inputKeys: 'not-an-array' as unknown as string[] }]), /array/);
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

test('selective invalidation leaves unchanged stages cached and passes only declared inputs', async () => {
  const cache = new ArtifactCache(new MemoryBackend());
  let meshRuns = 0;
  let shadeRuns = 0;
  let summaryRuns = 0;
  let unkeyedRuns = 0;

  let passedToShade: Record<string, unknown> = {};
  let passedToSummary: Record<string, unknown> = {};

  const stages: Stage[] = [
    {
      id: 'mesh',
      version: 1,
      dependencies: [],
      inputKeys: ['geometry', 'resolution'],
      run: (inputs) => {
        meshRuns++;
        return Float32Array.from(inputs.geometry as Float32Array, (x) => x * Number(inputs.resolution ?? 1));
      },
    },
    {
      id: 'shade',
      version: 1,
      dependencies: ['mesh'],
      inputKeys: ['sun', 'optionalMissing'],
      run: (inputs, upstream) => {
        shadeRuns++;
        passedToShade = inputs;
        const mesh = upstream.mesh as Float32Array;
        return Float32Array.from(mesh, (h) => h * Number(inputs.sun));
      },
    },
    {
      id: 'summary',
      version: 1,
      dependencies: ['mesh'],
      inputKeys: [],
      run: (inputs, upstream) => {
        summaryRuns++;
        passedToSummary = inputs;
        const mesh = upstream.mesh as Float32Array;
        return { count: mesh.length, sum: mesh.reduce((a, b) => a + b, 0) };
      },
    },
    {
      id: 'unkeyed',
      version: 1,
      dependencies: [],
      // inputKeys omitted: reads all root inputs
      run: (inputs) => {
        unkeyedRuns++;
        return Object.keys(inputs).sort().join(',');
      },
    },
  ];

  const pipeline = new Pipeline(cache, 'terrain', stages);
  const initialInputs = {
    geometry: new Float32Array([1, 2, 3]),
    resolution: 2,
    sun: 0.5,
    extra: 'ignored-by-keyed',
  };

  // Run 1: Initial full build
  const res1 = await pipeline.run(initialInputs);
  await cache.flush();
  assert.deepEqual(res1.built.sort(), ['mesh', 'shade', 'summary', 'unkeyed']);
  assert.deepEqual(res1.cacheHits, []);
  assert.equal(meshRuns, 1);
  assert.equal(shadeRuns, 1);
  assert.equal(summaryRuns, 1);
  assert.equal(unkeyedRuns, 1);
  // shade received only declared 'sun' (optionalMissing was omitted and absent)
  assert.deepEqual(passedToShade, { sun: 0.5 });
  assert.equal('optionalMissing' in passedToShade, false);
  assert.equal('geometry' in passedToShade, false);
  // summary received {} (inputKeys: [])
  assert.deepEqual(passedToSummary, {});

  // Run 2: Changing only display/lighting setting 'sun'
  const res2 = await pipeline.run({ ...initialInputs, sun: 0.9 });
  await cache.flush();
  assert.deepEqual(res2.built.sort(), ['shade', 'unkeyed']);
  assert.deepEqual(res2.cacheHits.sort(), ['mesh', 'summary']);
  assert.equal(meshRuns, 1); // Not re-run!
  assert.equal(shadeRuns, 2); // Re-run!
  assert.equal(summaryRuns, 1); // Not re-run!
  assert.equal(unkeyedRuns, 2); // Re-run because unkeyed takes all root inputs

  // Run 3: Changing 'geometry' invalidates mesh, shade, and summary (dependents), but unkeyed stays cached if root inputs unchanged
  const res3 = await pipeline.run({ ...initialInputs, geometry: new Float32Array([4, 5, 6]) });
  await cache.flush();
  assert.deepEqual(res3.built.sort(), ['mesh', 'shade', 'summary', 'unkeyed']);
  assert.equal(meshRuns, 2);
  assert.equal(shadeRuns, 3);
  assert.equal(summaryRuns, 2);
});

test('upstream version changes invalidate dependent stages even if upstream output is identical', async () => {
  const cache = new ArtifactCache(new MemoryBackend());
  let downstreamRuns = 0;

  const stageA1: Stage = {
    id: 'producerA',
    version: 1,
    dependencies: [],
    inputKeys: [],
    run: () => ({ fixed: 42 }),
  };
  const stageB: Stage = {
    id: 'consumerB',
    version: 1,
    dependencies: ['producerA'],
    inputKeys: [],
    run: (_, upstream) => {
      downstreamRuns++;
      return (upstream.producerA as { fixed: number }).fixed * 2;
    },
  };

  const p1 = new Pipeline(cache, 'version-test', [stageA1, stageB]);
  const res1 = await p1.run({});
  await cache.flush();
  assert.deepEqual(res1.built, ['producerA', 'consumerB']);
  assert.equal(downstreamRuns, 1);
  assert.equal(res1.values.consumerB, 84);

  // Producer A version changes to 2, but its output is identical ({ fixed: 42 })
  const stageA2: Stage = {
    id: 'producerA',
    version: 2,
    dependencies: [],
    inputKeys: [],
    run: () => ({ fixed: 42 }),
  };

  const p2 = new Pipeline(cache, 'version-test', [stageA2, stageB]);
  const res2 = await p2.run({});
  await cache.flush();
  assert.deepEqual(res2.built, ['producerA', 'consumerB']);
  assert.equal(downstreamRuns, 2); // Consumer B was invalidated and rebuilt!
  assert.equal(res2.values.consumerB, 84);
});

test('reopening FileBackend restores cache and supports selective reuse', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'artifact-cache-pipeline-'));
  try {
    const stages: Stage[] = [
      { id: 'geo', version: 1, dependencies: [], inputKeys: ['points'], run: ({ points }) => Float32Array.from(points as Float32Array, (x) => x * 2) },
      { id: 'render', version: 1, dependencies: ['geo'], inputKeys: ['fov'], run: ({ fov }, upstream) => `${(upstream.geo as Float32Array).length}@${fov}` },
    ];

    const cache1 = new ArtifactCache(new FileBackend(directory));
    const p1 = new Pipeline(cache1, 'project-file', stages);
    const inputs1 = { points: new Float32Array([10, 20, 30]), fov: 75 };
    const r1 = await p1.run(inputs1);
    await cache1.flush();
    assert.deepEqual(r1.built, ['geo', 'render']);
    assert.equal(r1.values.render, '3@75');

    // Reopen from disk with a fresh FileBackend instance
    const cache2 = new ArtifactCache(new FileBackend(directory));
    const p2 = new Pipeline(cache2, 'project-file', stages);

    // Warm run with same inputs
    const r2 = await p2.run(inputs1);
    assert.deepEqual(r2.cacheHits, ['geo', 'render']);
    assert.equal(r2.values.render, '3@75');

    // Selective reuse after modifying only 'fov'
    const r3 = await p2.run({ ...inputs1, fov: 90 });
    await cache2.flush();
    assert.deepEqual(r3.cacheHits, ['geo']);
    assert.deepEqual(r3.built, ['render']);
    assert.equal(r3.values.render, '3@90');
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('consistent keys for equivalent inputs (field order, view slice, negative zero, undefined)', async () => {
  const cache = new ArtifactCache(new MemoryBackend());
  let runs = 0;
  const stages: Stage[] = [
    {
      id: 'canon',
      version: 1,
      dependencies: [],
      inputKeys: ['config', 'buffer', 'num'],
      run: (inputs) => {
        runs++;
        return { ok: true, inputs };
      },
    },
  ];

  const pipeline = new Pipeline(cache, 'canonical-test', stages);

  // Run 1: Original object and typed array
  const buf1 = new Float32Array([1, 2, 3]);
  const input1 = {
    config: { b: 2, a: 1, nested: { y: 'y', x: 'x' } },
    buffer: buf1,
    num: -0,
    omitted: undefined,
  };
  const r1 = await pipeline.run(input1);
  await cache.flush();
  assert.equal(r1.built.length, 1);
  assert.equal(runs, 1);

  // Run 2: Reordered object properties and typed array slice with offset in larger buffer
  const backing = new Float32Array([999, 1, 2, 3, 888]);
  const buf2 = backing.subarray(1, 4); // View over [1, 2, 3]
  const input2 = {
    num: -0,
    buffer: buf2,
    config: { nested: { x: 'x', y: 'y' }, a: 1, b: 2 },
  };
  const r2 = await pipeline.run(input2);
  assert.deepEqual(r2.cacheHits, ['canon']);
  assert.equal(runs, 1); // No second execution!

  // Run 3: Distinguish +0 from -0
  const r3 = await pipeline.run({ ...input2, num: +0 });
  assert.deepEqual(r3.built, ['canon']);
  assert.equal(runs, 2);

  // Run 4: Distinguish typed-array kind (Int32Array vs Float32Array)
  const r4 = await pipeline.run({ ...input2, buffer: new Int32Array([1, 2, 3]) });
  assert.deepEqual(r4.built, ['canon']);
  assert.equal(runs, 3);
});

test('adding an unrelated stage or changing registration order does not invalidate existing results', async () => {
  const cache = new ArtifactCache(new MemoryBackend());

  const stageA: Stage = { id: 'stageA', version: 1, dependencies: [], inputKeys: ['a'], run: ({ a }) => `a:${a}` };
  const stageB: Stage = { id: 'stageB', version: 1, dependencies: [], inputKeys: ['b'], run: ({ b }) => `b:${b}` };
  const stageC: Stage = { id: 'stageC', version: 1, dependencies: [], inputKeys: ['c'], run: ({ c }) => `c:${c}` };

  const p1 = new Pipeline(cache, 'order-test', [stageA, stageB]);
  const r1 = await p1.run({ a: 1, b: 2 });
  await cache.flush();
  assert.deepEqual(r1.built.sort(), ['stageA', 'stageB']);

  // Different registration order and adding unrelated stageC
  const p2 = new Pipeline(cache, 'order-test', [stageC, stageB, stageA]);
  const r2 = await p2.run({ a: 1, b: 2, c: 3 });
  await cache.flush();
  assert.deepEqual(r2.cacheHits.sort(), ['stageA', 'stageB']);
  assert.deepEqual(r2.built, ['stageC']);
});

test('simultaneous pipeline runs share in-flight computations', async () => {
  const cache = new ArtifactCache(new MemoryBackend());
  let computeCount = 0;

  const stage: Stage = {
    id: 'heavy',
    version: 1,
    dependencies: [],
    inputKeys: ['val'],
    run: async ({ val }) => {
      computeCount++;
      await new Promise((resolve) => setTimeout(resolve, 30));
      return new Float32Array([Number(val) * 10]);
    },
  };

  const p1 = new Pipeline(cache, 'shared-run', [stage]);
  const p2 = new Pipeline(cache, 'shared-run', [stage]);

  const [res1, res2] = await Promise.all([
    p1.run({ val: 5 }),
    p2.run({ val: 5 }),
  ]);

  assert.equal(computeCount, 1);
  assert.deepEqual(res1.built, ['heavy']);
  assert.deepEqual(res2.built, ['heavy']);
  assert.deepEqual(res1.values.heavy, new Float32Array([50]));
  assert.deepEqual(res2.values.heavy, new Float32Array([50]));

  // Verify result isolation between concurrent pipeline callers
  (res1.values.heavy as Float32Array)[0] = 999;
  assert.equal((res2.values.heavy as Float32Array)[0], 50);
});

test('declared "__proto__" input fields stay ordinary data in projection and keys', async () => {
  const cache = new ArtifactCache(new MemoryBackend());
  let runs = 0;
  const pipeline = new Pipeline(cache, 'special-names', [{
    id: 'copy', version: 1, dependencies: [], inputKeys: ['__proto__', 'nested'],
    run: (inputs) => { runs++; return inputs; },
  }]);

  const first = await pipeline.run(JSON.parse('{"__proto__":{"n":3},"nested":{"__proto__":{"b":2,"a":1}},"ignored":1}'));
  await cache.flush();
  assert.deepEqual(first.built, ['copy']);
  assert.deepEqual(first.values.copy, JSON.parse('{"__proto__":{"n":3},"nested":{"__proto__":{"b":2,"a":1}}}'));

  // Reordered nested fields and an undeclared edit reuse the saved result.
  const second = await pipeline.run(JSON.parse('{"ignored":2,"nested":{"__proto__":{"a":1,"b":2}},"__proto__":{"n":3}}'));
  assert.deepEqual(second.cacheHits, ['copy']);

  const third = await pipeline.run(JSON.parse('{"__proto__":{"n":4},"nested":{"__proto__":{"a":1,"b":2}}}'));
  assert.deepEqual(third.built, ['copy']);
  assert.equal(runs, 2);
});
