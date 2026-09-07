// Copyright 2026 citrustiara. SPDX-License-Identifier: Apache-2.0
// The initial pipeline keeps RoadForge's conservative whole-build revision:
// correctness before selective reuse. It does not infer what a function reads.
import { ArtifactCache } from './cache.js';
import { encodeArtifact, snapshot, validateFormat } from './codec.js';
import { sha256 } from './key.js';

export type Inputs = Readonly<Record<string, unknown>>;
export interface Stage<T = unknown> {
  id: string;
  version: number;
  dependencies: readonly string[];
  run(inputs: Inputs, upstream: Readonly<Record<string, unknown>>): Promise<T> | T;
}
export interface PipelineResult {
  values: Record<string, unknown>;
  built: string[];
  cacheHits: string[];
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
      this.stages.set(stage.id, { ...stage, dependencies: [...stage.dependencies] });
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
    const revision = await sha256(encodeArtifact({
      inputs: owned,
      stages: [...this.stages.values()].map((stage) => [stage.id, stage.version, stage.dependencies]),
    }, { producer: 'artifact-cache/build-inputs', version: 1 }));
    const values: Record<string, unknown> = Object.create(null) as Record<string, unknown>;
    const built: string[] = [];
    const cacheHits: string[] = [];
    for (const stage of order) {
      const upstream = Object.fromEntries(stage.dependencies.map((id) => [id, snapshot(values[id])]));
      const result = await this.cache.getOrCompute({
        project: this.project, owner: stage.id, format: { producer: stage.id, version: stage.version }, revision,
      }, () => stage.run(snapshot(owned), upstream));
      values[stage.id] = snapshot(result.value);
      (result.cacheHit ? cacheHits : built).push(stage.id);
    }
    return { values, built, cacheHits };
  }
}
