import assert from 'node:assert/strict';
import test from 'node:test';
import { decodeArtifact, encodeArtifact, FormatMismatch, snapshot } from '../src/index.js';
const format = { producer: 'sample', version: 1 };

test('typed arrays preserve kinds, view boundaries, and values', () => {
  const source = { arrays: [
    new Float32Array([1.5, NaN, -0]), new Float64Array([Math.PI, Infinity, -Infinity]),
    new Int32Array([-2147483648, 2147483647]), new Uint32Array([4294967295]),
    new Int16Array([-32768]), new Uint16Array([65535]), new Int8Array([-128]),
    new Uint8Array([255]), new Uint8ClampedArray([0, 256]),
    new Float32Array([999, 0.25, 0.5, 888]).subarray(1, 3),
  ] };
  assert.deepEqual(snapshot(source), source);
  const packed = encodeArtifact(source, format);
  source.arrays[9][0] = 9;
  const restored = decodeArtifact<typeof source>(packed, format);
  assert.equal(restored.arrays[9][0], 0.25);
});

test('special numbers and marker-looking properties round trip', () => {
  const source = JSON.parse('{"__proto__":{"polluted":true},"$rf":2,"$rfn":"NaN"}');
  const value = { source, n: NaN, p: Infinity, m: -Infinity, z: -0, list: [undefined], omit: undefined };
  const restored = snapshot(value);
  assert.deepEqual(restored, { source, n: NaN, p: Infinity, m: -Infinity, z: -0, list: [null] });
  assert.equal(Object.hasOwn(restored.source, '__proto__'), true);
  assert.equal(({} as Record<string, unknown>).polluted, undefined);
});

test('wrong format, truncated data and a narrow view cannot silently decode', () => {
  const packed = encodeArtifact({ samples: new Float64Array([1, 2]) }, format);
  assert.throws(() => decodeArtifact(packed, { ...format, version: 2 }), FormatMismatch);
  assert.throws(() => decodeArtifact(packed, { ...format, producer: 'other' }), FormatMismatch);
  for (const length of [0, 4, 11, 20, packed.length - 1]) {
    assert.throws(() => decodeArtifact(packed.subarray(0, length), format));
  }
  const backing = new Uint8Array(packed.length + 20);
  backing.set(packed, 10);
  assert.deepEqual(decodeArtifact(backing.subarray(10, 10 + packed.length), format), { samples: new Float64Array([1, 2]) });
});

test('cycles and unsupported values fail clearly, shared plain values are copied', () => {
  const cycle: unknown[] = []; cycle.push(cycle);
  for (const value of [cycle, new Map(), new Date(), 1n, () => 1, new DataView(new ArrayBuffer(4))]) {
    assert.throws(() => encodeArtifact(value, format), TypeError);
  }
  const shared = { n: 2 };
  const restored = snapshot({ a: shared, b: shared });
  assert.deepEqual(restored.a, restored.b);
  assert.notEqual(restored.a, restored.b);
});
