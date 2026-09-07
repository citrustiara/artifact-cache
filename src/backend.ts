// Copyright 2026 citrustiara. SPDX-License-Identifier: Apache-2.0
export interface EntryMeta {
  key: string;
  /** Encoded artifact payload bytes, excluding backend bookkeeping. */
  bytes: number;
  usedAt: number;
}

/**
 * Backend boundary. Writes atomically replace one key. A successful list()
 * returns only complete entries. Implementations must not retain mutable
 * references to caller bytes or expose their own internal bytes from read().
 */
export interface StorageBackend {
  list(): Promise<EntryMeta[]>;
  read(key: string): Promise<Uint8Array | undefined>;
  write(key: string, bytes: Uint8Array, usedAt: number): Promise<void>;
  touch(key: string, usedAt: number): Promise<void>;
  remove(key: string): Promise<void>;
}

export function validateKey(key: string): void {
  if (!/^[a-f0-9]{64}$/.test(key)) throw new TypeError('Expected a SHA-256 cache key');
}
