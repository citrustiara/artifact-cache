// Copyright 2026 citrustiara. SPDX-License-Identifier: Apache-2.0
// Adapted from RoadForge's scene cache: isolated instances and backend injection
// replace application singletons and browser/native runtime detection.
import { decodeArtifact, encodeArtifact, snapshot } from './codec.js';
import { cacheKey, type CacheIdentity } from './key.js';
import type { EntryMeta, StorageBackend } from './backend.js';

export interface CacheOptions {
  budgetBytes?: number;
  maxQueuedBytes?: number;
  now?: () => number;
  onError?: (error: unknown) => void;
}
export interface CacheStats {
  hits: number;
  misses: number;
  writes: number;
  evictions: number;
  dropped: number;
  errors: number;
  queuedBytes: number;
  peakQueuedBytes: number;
  storedBytes: number;
  entries: number;
}
export interface CachedResult<T> { value: T; cacheHit: boolean }

function budget(value: number | undefined, fallback: number): number {
  const result = value ?? fallback;
  if (!Number.isSafeInteger(result) || result < 0) throw new TypeError('Byte budgets must be nonnegative integers');
  return result;
}
const copyIdentity = (identity: CacheIdentity): CacheIdentity => ({ ...identity, format: { ...identity.format } });

/**
 * Best-effort persistent cache. Storage/hash/codec failures are misses or dropped
 * writes; producer exceptions still reject. The queue owns encoded snapshots,
 * counts them before asynchronous key derivation, and never exceeds its cap.
 * One instance should own a backend; no cross-process budget locking is claimed.
 */
export class ArtifactCache {
  private readonly index = new Map<string, EntryMeta>();
  private loaded = false;
  private tail: Promise<void> = Promise.resolve();
  private readonly inFlight = new Map<string, Promise<CachedResult<unknown>>>();
  private readonly budgetBytes: number;
  private readonly maxQueuedBytes: number;
  private readonly now: () => number;
  private readonly counters = { hits: 0, misses: 0, writes: 0, evictions: 0, dropped: 0, errors: 0, queuedBytes: 0, peakQueuedBytes: 0 };

  constructor(private readonly backend: StorageBackend, private readonly options: CacheOptions = {}) {
    this.budgetBytes = budget(options.budgetBytes, 256 * 1024 * 1024);
    this.maxQueuedBytes = budget(options.maxQueuedBytes, 16 * 1024 * 1024);
    this.now = options.now ?? Date.now;
  }

  private report(error: unknown): void {
    this.counters.errors++;
    try { this.options.onError?.(error); } catch { /* Diagnostics cannot break a computation. */ }
  }
  private enqueue<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.tail.then(operation);
    // A failed operation must not poison the next read/write.
    this.tail = result.then(() => undefined, () => undefined);
    return result;
  }
  private async load(): Promise<void> {
    if (this.loaded) return;
    const entries = await this.backend.list();
    this.index.clear();
    for (const entry of entries) {
      if (/^[a-f0-9]{64}$/.test(entry.key) && Number.isSafeInteger(entry.bytes) && entry.bytes >= 0 && Number.isFinite(entry.usedAt)) {
        this.index.set(entry.key, { ...entry });
      }
    }
    this.loaded = true;
    await this.evict(0);
  }
  private storedBytes(): number {
    let bytes = 0;
    for (const entry of this.index.values()) bytes += entry.bytes;
    return bytes;
  }
  private async evict(incoming: number, replacing?: string): Promise<void> {
    let total = this.storedBytes() - (replacing ? this.index.get(replacing)?.bytes ?? 0 : 0) + incoming;
    const candidates = [...this.index.values()].filter((entry) => entry.key !== replacing)
      .sort((a, b) => a.usedAt - b.usedAt || a.key.localeCompare(b.key));
    for (const entry of candidates) {
      if (total <= this.budgetBytes) break;
      await this.backend.remove(entry.key);
      this.index.delete(entry.key);
      total -= entry.bytes;
      this.counters.evictions++;
    }
  }

  get<T>(identity: CacheIdentity): Promise<T | undefined> {
    const owned = copyIdentity(identity);
    return this.enqueue(async () => {
      let key: string | undefined;
      try {
        key = await cacheKey(owned);
        await this.load();
        if (!this.index.has(key)) { this.counters.misses++; return undefined; }
        const bytes = await this.backend.read(key);
        if (!bytes) throw new Error('Cache index has no payload');
        const value = decodeArtifact<T>(bytes, owned.format);
        const usedAt = this.now();
        this.index.set(key, { key, bytes: bytes.byteLength, usedAt });
        try { await this.backend.touch(key, usedAt); } catch (error) { this.report(error); }
        this.counters.hits++;
        return value;
      } catch (error) {
        this.report(error);
        this.counters.misses++;
        if (key) this.index.delete(key);
        return undefined;
      }
    });
  }

  put(identity: CacheIdentity, value: unknown): Promise<void> {
    const owned = copyIdentity(identity);
    let bytes: Uint8Array;
    try { bytes = encodeArtifact(value, owned.format); }
    catch (error) { this.report(error); this.counters.dropped++; return Promise.resolve(); }
    if (bytes.byteLength > this.budgetBytes || this.counters.queuedBytes + bytes.byteLength > this.maxQueuedBytes) {
      this.counters.dropped++;
      return Promise.resolve();
    }
    this.counters.queuedBytes += bytes.byteLength;
    this.counters.peakQueuedBytes = Math.max(this.counters.peakQueuedBytes, this.counters.queuedBytes);
    return this.enqueue(async () => {
      try {
        const key = await cacheKey(owned);
        await this.load();
        await this.evict(bytes.byteLength, key);
        const usedAt = this.now();
        await this.backend.write(key, bytes, usedAt);
        this.index.set(key, { key, bytes: bytes.byteLength, usedAt });
        this.counters.writes++;
      } catch (error) { this.report(error); this.counters.dropped++; }
      finally { this.counters.queuedBytes -= bytes.byteLength; }
    });
  }

  /**
   * Read or compute. Writes are queued, not awaited on the producer's path.
   * Simultaneous calls with the same identity share one read or computation;
   * each caller gets its own copy, and the entry is removed once it settles.
   */
  async getOrCompute<T>(identity: CacheIdentity, produce: () => Promise<T> | T): Promise<CachedResult<T>> {
    const owned = copyIdentity(identity);
    // Share by storage key. Without a key, compute unshared; get() reports the error.
    let inFlightKey: string | undefined;
    try { inFlightKey = await cacheKey(owned); } catch { inFlightKey = undefined; }

    let inFlightPromise = inFlightKey === undefined ? undefined : this.inFlight.get(inFlightKey) as Promise<CachedResult<T>> | undefined;
    if (!inFlightPromise) {
      const execute = async (): Promise<CachedResult<T>> => {
        try {
          const cached = await this.get<T>(owned);
          if (cached !== undefined) return { value: cached, cacheHit: true };
          const value = await produce();
          if (value === undefined) throw new TypeError('A producer must not return undefined');
          void this.put(owned, value);
          return { value, cacheHit: false };
        } finally {
          if (inFlightKey !== undefined) this.inFlight.delete(inFlightKey);
        }
      };

      inFlightPromise = execute();
      if (inFlightKey !== undefined) this.inFlight.set(inFlightKey, inFlightPromise);
    }

    const result = await inFlightPromise;
    return { value: snapshot(result.value), cacheHit: result.cacheHit };
  }

  /** Drains accepted writes, including key derivation, until the queue is stable. */
  async flush(): Promise<void> {
    let observed: Promise<void>;
    do { observed = this.tail; await observed; } while (observed !== this.tail);
  }
  stats(): CacheStats {
    return { ...this.counters, storedBytes: this.storedBytes(), entries: this.index.size };
  }
}
