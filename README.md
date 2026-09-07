# Artifact Cache

A small TypeScript library for keeping expensive numerical results between runs. It stores plain data and typed arrays in versioned binary artifacts, with a byte-budgeted cache and a simple dependency-ordered pipeline runner.

Typical uses include terrain preprocessing, scientific visualization, geometry tools, and local analysis applications. You provide the computations; the library provides persistence. It is not a renderer, a distributed build system, or a job scheduler.

## Install from source

Requires Node.js 22 or later. This project is available from GitHub; it has not been published to npm.

```sh
git clone https://github.com/citrustiara/artifact-cache.git
cd artifact-cache
npm ci
npm run check
npm run demo
npm run demo
```

The first demo builds three synthetic terrain-processing stages. The second restores them from `.artifact-cache/`. Delete that directory to start fresh, or set `ARTIFACT_CACHE_DIR` to use another location. No external data is downloaded by the demo.

For use in another local project, run `npm run build` here, then `npm install /path/to/artifact-cache` there. The package exports compiled ESM and TypeScript declarations.

## Cache a computation

```ts
import { ArtifactCache } from '@citrustiara/artifact-cache';
import { FileBackend } from '@citrustiara/artifact-cache/file';

const cache = new ArtifactCache(new FileBackend('./cached-results'), {
  budgetBytes: 128 * 1024 * 1024,
  maxQueuedBytes: 16 * 1024 * 1024,
});

const result = await cache.getOrCompute({
  project: 'experiment-1',
  owner: 'filtered-signal',
  format: { producer: 'low-pass', version: 1 },
  revision: 'recording-42/filter-settings-3',
}, () => new Float32Array([0.25, 0.5, 0.75]));

console.log(result.cacheHit, result.value);
await cache.flush();
```

The revision is your responsibility: it must identify every input affecting the computation. Increment the format version whenever the producer's output meaning or layout changes. Project and owner names are part of the identity, not filenames.

`getOrCompute` reports whether the result came from persistent storage. Producer exceptions reject normally. Cache I/O, hashing, or decoding failures instead cause recomputation or a dropped write; observe them through `onError` and `stats()` if needed.

Writes snapshot their data immediately and queue behind the computation. `flush()` waits for accepted writes, including key derivation. An artifact larger than the storage budget, or a write that would exceed the queued-byte cap, is not retained. It can still be returned by the producer. LRU eviction uses encoded payload size; filesystem overhead is not included.

## Run dependent stages

```ts
import { ArtifactCache, MemoryBackend, Pipeline } from '@citrustiara/artifact-cache';

const cache = new ArtifactCache(new MemoryBackend());
const pipeline = new Pipeline(cache, 'example', [
  { id: 'scaled', version: 1, dependencies: [],
    run: ({ samples, scale }) => Float32Array.from(samples as Float32Array, x => x * Number(scale)) },
  { id: 'total', version: 1, dependencies: ['scaled'],
    run: (_, upstream) => (upstream.scaled as Float32Array).reduce((sum, x) => sum + x, 0) },
]);

const result = await pipeline.run({ samples: new Float32Array([1, 2, 3]), scale: 2 });
await cache.flush();
console.log(result.values.total); // 12
```

A pipeline snapshots inputs at call time, checks the graph for missing dependencies and cycles, and runs stages in dependency order. Optional targets restrict execution: `pipeline.run(inputs, ['total'])`. Results include the dependency closure, a `built` list, and a `cacheHits` list. Producers receive isolated copies of inputs and dependency outputs; producer functions must be deterministic and use no hidden mutable state.

Version 0.1 uses a conservative whole-build revision: any changed input or stage definition invalidates the pipeline's cached stages. It favors correctness over fine-grained reuse. It does not normalize object-property order or coalesce overlapping computations. Use explicit revisions with the lower-level cache when you already have a reliable per-artifact identity.

## Data and storage boundaries

- The codec supports null, booleans, strings, numbers (including NaN, infinities, and negative zero), arrays, plain objects, and nine numerical typed-array kinds, including clamped bytes. Only the bytes inside a typed-array view are stored.
- Undefined object fields are omitted; undefined array elements become null. Cycles, class instances, functions, bigint, and DataView are rejected. Value copies are preserved, not alias relationships. Producers must not return undefined.
- Packed artifacts carry producer/version stamps and buffer bounds checks. They are cache data, not a cryptographically authenticated or sandboxed interchange format. Keep the storage directory under your application's control.
- `MemoryBackend` is useful for tests and ephemeral sessions. `FileBackend` atomically replaces each entry and persists across Node processes. Use one owning cache per directory; cross-process coordination is not provided.
- The main export has no Node-specific imports and requires Web Crypto. A browser application can implement `StorageBackend` with IndexedDB; no browser backend is bundled yet.
- Persistence is best effort: callers should always be able to regenerate artifacts. `flush()` is orderly shutdown support, not a guarantee against every filesystem or power-loss failure.

## Development

```sh
npm run typecheck
npm test
npm run build
```

The tests use synthetic arrays, temporary directories, and injected failing backends. No hardware, browser, or network is required after `npm ci`.

## License and origin

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE). The binary packing and cache design were extracted and adapted from RoadForge. This repository contains no application renderer, map assets, or geographic datasets.
