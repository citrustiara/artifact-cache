import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, rm, writeFile, readdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ArtifactCache, cacheKey } from '../src/index.js';
import { FileBackend } from '../src/backends/file.js';
const identity = { project: 'p', owner: 'value', revision: 'r', format: { producer: 'value', version: 1 } };

test('file backend survives restart, snapshots view bytes and leaves no temporary files', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'artifact-cache-'));
  try {
    const cache = new ArtifactCache(new FileBackend(directory));
    await cache.put(identity, { values: new Uint16Array([9, 1, 2, 9]).subarray(1, 3) });
    await cache.flush();
    const backend = new FileBackend(directory);
    assert.equal((await backend.list()).length, 1);
    const reopened = new ArtifactCache(backend);
    assert.deepEqual(await reopened.get(identity), { values: new Uint16Array([1, 2]) });
    assert.equal((await readdir(directory)).filter((name) => name.endsWith('.tmp')).length, 0);
    await backend.remove(await cacheKey(identity));
    assert.deepEqual(await backend.list(), []);
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('partial files are ignored and path-like keys are refused', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'artifact-cache-'));
  try {
    const key = await cacheKey(identity);
    await writeFile(join(directory, `${key}.artifact`), new Uint8Array([255, 255, 255, 255]));
    const backend = new FileBackend(directory);
    assert.deepEqual(await backend.list(), []);
    await assert.rejects(backend.read('../outside'));
    const cache = new ArtifactCache(backend);
    assert.equal((await cache.getOrCompute(identity, () => 4)).value, 4);
    await cache.flush();
    assert.equal(await cache.get(identity), 4);
  } finally { await rm(directory, { recursive: true, force: true }); }
});
