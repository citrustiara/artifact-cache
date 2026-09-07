// Copyright 2026 citrustiara. SPDX-License-Identifier: Apache-2.0
import { validateKey, type EntryMeta, type StorageBackend } from '../backend.js';

/** In-memory backend. Reuse the instance across cache instances to emulate restart. */
export class MemoryBackend implements StorageBackend {
  private readonly entries = new Map<string, { bytes: Uint8Array; usedAt: number }>();

  async list(): Promise<EntryMeta[]> {
    return [...this.entries].map(([key, entry]) => ({ key, bytes: entry.bytes.byteLength, usedAt: entry.usedAt }));
  }
  async read(key: string): Promise<Uint8Array | undefined> {
    validateKey(key);
    return this.entries.get(key)?.bytes.slice();
  }
  async write(key: string, bytes: Uint8Array, usedAt: number): Promise<void> {
    validateKey(key);
    this.entries.set(key, { bytes: bytes.slice(), usedAt });
  }
  async touch(key: string, usedAt: number): Promise<void> {
    validateKey(key);
    const entry = this.entries.get(key);
    if (entry) entry.usedAt = usedAt;
  }
  async remove(key: string): Promise<void> {
    validateKey(key);
    this.entries.delete(key);
  }
}
