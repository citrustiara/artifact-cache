import { ArtifactCache, Pipeline, type Stage } from '../src/index.js';
import { FileBackend } from '../src/backends/file.js';

// Synthetic data only: no maps, downloads, renderer, or GIS dependency.
const stages: Stage[] = [
  { id: 'surface', version: 1, dependencies: [], inputKeys: ['heights', 'scale'], run: ({ heights, scale }) =>
    Float32Array.from(heights as Float32Array, (height) => height * Number(scale)) },
  { id: 'shade', version: 1, dependencies: ['surface'], inputKeys: ['sun'], run: ({ sun }, upstream) =>
    Float32Array.from(upstream.surface as Float32Array, (height) => Math.max(0, 1 - height * Number(sun))) },
  { id: 'summary', version: 1, dependencies: ['surface'], inputKeys: [], run: (_, upstream) => {
    const heights = upstream.surface as Float32Array;
    return { minimum: Math.min(...heights), maximum: Math.max(...heights), samples: heights.length };
  } },
];

const cache = new ArtifactCache(new FileBackend(process.env.ARTIFACT_CACHE_DIR ?? '.artifact-cache'));
const pipeline = new Pipeline(cache, 'synthetic-terrain', stages);

const initialInputs = { heights: new Float32Array([0.1, 0.5, 0.2, 0.8]), scale: 2, sun: 0.4 };
const first = await pipeline.run(initialInputs);
await cache.flush();
console.log('Initial build:');
console.log(JSON.stringify({ built: first.built, hits: first.cacheHits, summary: first.values.summary }, null, 2));

// Changing only the lighting setting (sun) reuses surface and summary:
const updatedInputs = { ...initialInputs, sun: 0.8 };
const second = await pipeline.run(updatedInputs);
await cache.flush();
console.log('After changing sun (display setting):');
console.log(JSON.stringify({ built: second.built, hits: second.cacheHits, summary: second.values.summary, stats: cache.stats() }, null, 2));
