export { ArtifactCache, type CacheOptions, type CacheStats, type CachedResult } from './cache.js';
export { MemoryBackend } from './backends/memory.js';
export { type StorageBackend, type EntryMeta } from './backend.js';
export { encodeArtifact, decodeArtifact, snapshot, FormatMismatch, type ArtifactFormat } from './codec.js';
export { cacheKey, type CacheIdentity } from './key.js';
export { Pipeline, type Stage, type Inputs, type PipelineResult } from './pipeline.js';
