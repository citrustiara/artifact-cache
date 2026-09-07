// Copyright 2026 citrustiara. SPDX-License-Identifier: Apache-2.0
// Adapted from RoadForge's artifact-codec: generic formats and an escaped tree
// replace its producer registry and reserved object-property markers.
import { parseRfPack, RfPackWriter } from './rfpack.js';

export interface ArtifactFormat {
  producer: string;
  /** Increment when the producer's output meaning or layout changes. */
  version: number;
}

export class FormatMismatch extends Error {}

export function validateFormat(format: ArtifactFormat): void {
  if (!format.producer || !Number.isSafeInteger(format.version) || format.version < 1) {
    throw new TypeError('A format needs a producer and a positive integer version');
  }
}

const typed = {
  Float32Array, Float64Array, Uint32Array, Int32Array, Uint16Array, Int16Array,
  Uint8Array, Int8Array, Uint8ClampedArray,
};
type ArrayName = keyof typeof typed;
type NumericArray = InstanceType<(typeof typed)[ArrayName]>;
const MAGIC = 'ARTIFACT_CACHE_1';
const COPY_FORMAT = { producer: 'artifact-cache/snapshot', version: 1 };

function arrayName(value: object): ArrayName | undefined {
  return (Object.keys(typed) as ArrayName[]).find((name) => value.constructor === typed[name]);
}

/**
 * A JSON skeleton with binary sections, not JSON arrays of decimal strings.
 * Object entries are escaped as tuples, so user keys such as "$rf" and
 * "__proto__" cannot be confused with serializer instructions.
 * Objects omit undefined fields; undefined array entries become null.
 * Cycles, functions, bigint, DataView, and class instances are unsupported.
 */
export function encodeArtifact(value: unknown, format: ArtifactFormat): Uint8Array {
  validateFormat(format);
  const pack = new RfPackWriter();
  const ancestors = new Set<object>();
  let section = 0;
  const walk = (value: unknown): unknown => {
    if (value === null || value === undefined) return ['null'];
    if (typeof value === 'string' || typeof value === 'boolean') return [typeof value, value];
    if (typeof value === 'number') {
      if (Number.isNaN(value)) return ['number', 'NaN'];
      if (value === Infinity) return ['number', '+Inf'];
      if (value === -Infinity) return ['number', '-Inf'];
      if (Object.is(value, -0)) return ['number', '-0'];
      return ['number', value];
    }
    if (typeof value !== 'object') throw new TypeError(`Cannot pack ${typeof value}`);
    const name = arrayName(value);
    if (name) {
      const view = value as NumericArray;
      const id = `b${section++}`;
      pack.add(id, new Uint8Array(view.buffer, view.byteOffset, view.byteLength));
      return ['buffer', name, id, view.length];
    }
    if (ancestors.has(value)) throw new TypeError('Cannot pack a cyclic value');
    ancestors.add(value);
    try {
      if (Array.isArray(value)) return ['array', Array.from(value, walk)];
      const prototype = Object.getPrototypeOf(value);
      if (prototype !== Object.prototype && prototype !== null) throw new TypeError('Cannot pack a class instance');
      return ['object', Object.entries(value)
        .filter(([, entry]) => entry !== undefined)
        .map(([key, entry]) => [key, walk(entry)])];
    } finally {
      ancestors.delete(value);
    }
  };
  pack.setMeta({ magic: MAGIC, ...format, tree: walk(value) });
  return pack.finish();
}

/** Throws on a wrong producer/version or malformed pack. Cache callers treat it as a miss. */
export function decodeArtifact<T = unknown>(bytes: Uint8Array, format: ArtifactFormat): T {
  validateFormat(format);
  const pack = parseRfPack(bytes);
  const header = pack.meta as { magic?: unknown; producer?: unknown; version?: unknown; tree?: unknown } | null;
  if (!header || header.magic !== MAGIC || header.producer !== format.producer || header.version !== format.version) {
    throw new FormatMismatch(`Expected ${format.producer} version ${format.version}`);
  }
  const bad = (): never => { throw new Error('Malformed artifact tree'); };
  const walk = (node: unknown): unknown => {
    if (!Array.isArray(node)) return bad();
    const [tag, value] = node;
    switch (tag) {
      case 'null': if (node.length === 1) return null; return bad();
      case 'string': if (node.length === 2 && typeof value === 'string') return value; return bad();
      case 'boolean': if (node.length === 2 && typeof value === 'boolean') return value; return bad();
      case 'number': {
        if (node.length !== 2) return bad();
        if (typeof value === 'number' && Number.isFinite(value)) return value;
        if (value === 'NaN') return NaN;
        if (value === '+Inf') return Infinity;
        if (value === '-Inf') return -Infinity;
        if (value === '-0') return -0;
        return bad();
      }
      case 'buffer': {
        const [, name, id, length] = node;
        if (node.length !== 4 || typeof name !== 'string' || !Object.hasOwn(typed, name) ||
            typeof id !== 'string' || !Number.isSafeInteger(length) || length < 0) return bad();
        const data = pack.sections.get(id);
        const constructor = typed[name as ArrayName];
        if (!(data instanceof Uint8Array) || data.byteLength !== length * constructor.BYTES_PER_ELEMENT) return bad();
        const buffer = data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) as ArrayBuffer;
        return new constructor(buffer);
      }
      case 'array':
        if (node.length !== 2 || !Array.isArray(value)) return bad();
        return value.map(walk);
      case 'object': {
        if (node.length !== 2 || !Array.isArray(value)) return bad();
        const out: Record<string, unknown> = {};
        for (const entry of value) {
          if (!Array.isArray(entry) || entry.length !== 2 || typeof entry[0] !== 'string' || Object.hasOwn(out, entry[0])) return bad();
          Object.defineProperty(out, entry[0], { value: walk(entry[1]), enumerable: true, writable: true, configurable: true });
        }
        return out;
      }
      default: return bad();
    }
  };
  return walk(header.tree) as T;
}

/** Value snapshot; preserves numerical bits, not shared-reference identity. */
export function snapshot<T>(value: T): T {
  return decodeArtifact<T>(encodeArtifact(value, COPY_FORMAT), COPY_FORMAT);
}
