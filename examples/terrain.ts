import { ArtifactCache, Pipeline, type Stage } from '../src/index.js';
import { FileBackend } from '../src/backends/file.js';

// Synthetic data only: no maps, downloads, renderer, or GIS dependency.
const stages: Stage[] = [
  { id: 'surface', version: 1, dependencies: [], run: ({ heights, scale }) =>
    Float32Array.from(heights as Float32Array, (height) => height * Number(scale)) },
  { id: 'shade', version: 1, dependencies: ['surface'], run: ({ sun }, upstream) =>
    Float32Array.from(upstream.surface as Float32Array, (height) => Math.max(0, 1 - height * Number(sun))) },
  { id: 'summary', version: 1, dependencies: ['surface'], run: (_, upstream) => {
    const heights = upstream.surface as Float32Array;
    return { minimum: Math.min(...heights), maximum: Math.max(...heights), samples: heights.length };
  } },
];

const cache = new ArtifactCache(new FileBackend(process.env.ARTIFACT_CACHE_DIR ?? '.artifact-cache'));
const pipeline = new Pipeline(cache, 'synthetic-terrain', stages);
const result = await pipeline.run({ heights: new Float32Array([0.1, 0.5, 0.2, 0.8]), scale: 2, sun: 0.4 });
await cache.flush();
console.log(JSON.stringify({ built: result.built, hits: result.cacheHits, summary: result.values.summary, stats: cache.stats() }, null, 2));
