// Copyright 2026 citrustiara. SPDX-License-Identifier: Apache-2.0
import { ArtifactCache } from './cache.js';
import { encodeArtifact, snapshot, validateFormat } from './codec.js';
import { cacheKey, sha256, type CacheIdentity } from './key.js';

export type Inputs = Readonly<Record<string, unknown>>;
export interface Stage<T = unknown> {
  id: string;
  version: number;
  dependencies: readonly string[];
  inputKeys?: readonly string[];
  run(inputs: Inputs, upstream: Readonly<Record<string, unknown>>): Promise<T> | T;
}
export interface PipelineResult {
  values: Record<string, unknown>;
  built: string[];
  cacheHits: string[];
}

function selectInputs(inputs: Record<string, unknown>, inputKeys?: readonly string[]): Record<string, unknown> {
  if (inputKeys === undefined) {
    return { ...inputs };
  }
  // fromEntries defines own fields, so a declared "__proto__" input stays data.
  return Object.fromEntries(inputKeys
    .filter((key) => Object.hasOwn(inputs, key) && inputs[key] !== undefined)
    .map((key) => [key, inputs[key]]));
}

/**
 * Key-only copy of a snapshot with plain-object fields sorted at every depth,
 * so equivalent inputs hash the same. Arrays and typed arrays keep their order
 * and kind; stored values and producer copies keep their own field order.
 */
function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (value === null || typeof value !== 'object' || ArrayBuffer.isView(value)) return value;
  return Object.fromEntries(Object.keys(value).sort()
    .map((key) => [key, canonical((value as Record<string, unknown>)[key])]));
}

/** A small local DAG runner, not a distributed task scheduler. */
export class Pipeline {
  private readonly stages = new Map<string, Stage>();

  constructor(private readonly cache: ArtifactCache, readonly project: string, stages: readonly Stage[]) {
    if (!project) throw new TypeError('A pipeline needs a project');
    for (const stage of stages) {
      validateFormat({ producer: stage.id, version: stage.version });
      if (this.stages.has(stage.id)) throw new TypeError(`Duplicate stage ${stage.id}`);
      if (new Set(stage.dependencies).size !== stage.dependencies.length) throw new TypeError(`Duplicate dependencies of ${stage.id}`);
      if (stage.inputKeys !== undefined) {
        if (!Array.isArray(stage.inputKeys)) {
          throw new TypeError(`inputKeys for stage ${stage.id} must be an array`);
        }
        const seen = new Set<string>();
        for (const key of stage.inputKeys) {
          if (typeof key !== 'string') {
            throw new TypeError(`inputKeys for stage ${stage.id} must contain only strings`);
          }
          if (seen.has(key)) {
            throw new TypeError(`Duplicate inputKey "${key}" in stage ${stage.id}`);
          }
          seen.add(key);
        }
      }
      this.stages.set(stage.id, {
        ...stage,
        dependencies: [...stage.dependencies],
        inputKeys: stage.inputKeys ? [...stage.inputKeys] : undefined,
      });
    }
    this.order([...this.stages.keys()]);
  }

  private order(targets: readonly string[]): Stage[] {
    const active = new Set<string>();
    const done = new Set<string>();
    const order: Stage[] = [];
    const visit = (id: string) => {
      const stage = this.stages.get(id);
      if (!stage) throw new TypeError(`Unknown stage ${id}`);
      if (active.has(id)) throw new TypeError(`Cycle through ${id}`);
      if (done.has(id)) return;
      active.add(id);
      for (const dependency of stage.dependencies) visit(dependency);
      active.delete(id);
      done.add(id);
      order.push(stage);
    };
    targets.forEach(visit);
    return order;
  }

  /**
   * Inputs are a call-time value snapshot. Every stage sees its own copy, and
   * downstream stages receive copies of dependency outputs. A mutation by one
   * producer therefore cannot rewrite another producer's view of this build.
   * Targets restrict execution to their transitive dependency closure.
   */
  async run(inputs: Inputs, targets: readonly string[] = [...this.stages.keys()]): Promise<PipelineResult> {
    const owned = snapshot(inputs);
    const order = this.order(targets);
    const stageKeys = new Map<string, string>();
    const values: Record<string, unknown> = Object.create(null) as Record<string, unknown>;
    const built: string[] = [];
    const cacheHits: string[] = [];

    for (const stage of order) {
      const stageInputs = selectInputs(owned, stage.inputKeys);
      const revisionBytes = encodeArtifact({
        inputs: canonical(stageInputs),
        dependencies: stage.dependencies.map((depId) => [depId, stageKeys.get(depId)!]),
      }, { producer: 'artifact-cache/stage-revision', version: 1 });
      const revision = await sha256(revisionBytes);

      const identity: CacheIdentity = {
        project: this.project,
        owner: stage.id,
        format: { producer: stage.id, version: stage.version },
        revision,
      };

      const key = await cacheKey(identity);
      stageKeys.set(stage.id, key);

      const upstream = Object.fromEntries(stage.dependencies.map((id) => [id, snapshot(values[id])]));
      const result = await this.cache.getOrCompute(identity, () => stage.run(snapshot(stageInputs), upstream));
      values[stage.id] = snapshot(result.value);
      (result.cacheHit ? cacheHits : built).push(stage.id);
    }

    return { values, built, cacheHits };
  }
}
