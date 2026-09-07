// Copyright 2026 citrustiara. SPDX-License-Identifier: Apache-2.0
import { validateFormat, type ArtifactFormat } from './codec.js';

export interface CacheIdentity {
  project: string;
  owner: string;
  format: ArtifactFormat;
  /** Caller-owned input revision. It must change when the computation changes. */
  revision: string;
}

export async function sha256(bytes: Uint8Array): Promise<string> {
  const digest = await globalThis.crypto.subtle.digest('SHA-256', bytes as Uint8Array<ArrayBuffer>);
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

/** Explicit tuple framing avoids separator collisions in user-controlled names. */
export function cacheKey(identity: CacheIdentity): Promise<string> {
  validateFormat(identity.format);
  if (!identity.project || !identity.owner || typeof identity.revision !== 'string') {
    throw new TypeError('A cache identity needs project, owner, and revision');
  }
  return sha256(new TextEncoder().encode(JSON.stringify([
    'artifact-cache/key-v1', identity.project, identity.owner,
    identity.format.producer, identity.format.version, identity.revision,
  ])));
}
