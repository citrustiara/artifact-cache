// Copyright 2026 citrustiara. SPDX-License-Identifier: Apache-2.0
import { mkdir, open, readdir, rename, unlink, utimes, type FileHandle } from 'node:fs/promises';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { validateKey, type EntryMeta, type StorageBackend } from '../backend.js';

const MAGIC = 'artifact-cache-file-v1';
const MAX_HEADER = 4096;
const absent = (error: unknown) => (error as NodeJS.ErrnoException).code === 'ENOENT';

async function readExactly(file: FileHandle, length: number, position: number): Promise<Uint8Array> {
  const bytes = new Uint8Array(length);
  let offset = 0;
  while (offset < length) {
    const { bytesRead } = await file.read(bytes, offset, length - offset, position + offset);
    if (bytesRead === 0) throw new Error('Truncated cache file');
    offset += bytesRead;
  }
  return bytes;
}

/**
 * Local Node backend, separately exported so the core has no Node imports.
 * Each entry is one atomically renamed file; readers never pair old metadata
 * with a new payload. Access time lives in mtime, avoiding a payload rewrite.
 * Intended for one owning ArtifactCache per directory, not distributed locking.
 */
export class FileBackend implements StorageBackend {
  constructor(readonly directory: string) {}

  private path(key: string): string {
    validateKey(key);
    return join(this.directory, `${key}.artifact`);
  }

  private async header(file: FileHandle, key: string): Promise<{ meta: EntryMeta; offset: number }> {
    const prefix = await readExactly(file, 4, 0);
    const length = new DataView(prefix.buffer).getUint32(0, true);
    if (length === 0 || length > MAX_HEADER) throw new Error('Invalid cache file header');
    const header = JSON.parse(new TextDecoder().decode(await readExactly(file, length, 4))) as Record<string, unknown>;
    const stat = await file.stat();
    if (header.magic !== MAGIC || header.key !== key || !Number.isSafeInteger(header.bytes) ||
        Number(header.bytes) < 0 || 4 + length + Number(header.bytes) !== stat.size) {
      throw new Error('Invalid cache file extent');
    }
    return { meta: { key, bytes: Number(header.bytes), usedAt: stat.mtimeMs }, offset: 4 + length };
  }

  async list(): Promise<EntryMeta[]> {
    await mkdir(this.directory, { recursive: true });
    const entries: EntryMeta[] = [];
    for (const name of (await readdir(this.directory)).sort()) {
      if (!/^[a-f0-9]{64}\.artifact$/.test(name)) continue;
      const key = name.slice(0, -9);
      let file: FileHandle | undefined;
      try {
        file = await open(this.path(key), 'r');
        entries.push((await this.header(file, key)).meta);
      } catch {
        // An interrupted/external writer may leave an unusable file. It is
        // never an index hit; the next write to its key replaces it atomically.
      } finally {
        await file?.close();
      }
    }
    return entries;
  }

  async read(key: string): Promise<Uint8Array | undefined> {
    let file: FileHandle;
    try { file = await open(this.path(key), 'r'); }
    catch (error) { if (absent(error)) return undefined; throw error; }
    try {
      const { meta, offset } = await this.header(file, key);
      return await readExactly(file, meta.bytes, offset);
    } finally { await file.close(); }
  }

  async write(key: string, bytes: Uint8Array, usedAt: number): Promise<void> {
    const destination = this.path(key);
    // Snapshot before the first await; even direct backend callers may mutate
    // or transfer the source buffer as soon as write() returns its promise.
    const owned = bytes.slice();
    const header = new TextEncoder().encode(JSON.stringify({ magic: MAGIC, key, bytes: owned.byteLength }));
    const prefix = new Uint8Array(4);
    new DataView(prefix.buffer).setUint32(0, header.byteLength, true);
    await mkdir(this.directory, { recursive: true });
    const temporary = `${destination}.${randomUUID()}.tmp`;
    try {
      const file = await open(temporary, 'wx', 0o600);
      try {
        await file.writeFile(prefix);
        await file.writeFile(header);
        await file.writeFile(owned);
        await file.sync();
      } finally { await file.close(); }
      const time = new Date(usedAt);
      await utimes(temporary, time, time);
      await rename(temporary, destination);
    } finally {
      await unlink(temporary).catch((error: unknown) => { if (!absent(error)) throw error; });
    }
  }

  async touch(key: string, usedAt: number): Promise<void> {
    const time = new Date(usedAt);
    await utimes(this.path(key), time, time).catch((error: unknown) => { if (!absent(error)) throw error; });
  }
  async remove(key: string): Promise<void> {
    await unlink(this.path(key)).catch((error: unknown) => { if (!absent(error)) throw error; });
  }
}
