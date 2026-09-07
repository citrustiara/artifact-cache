// Copyright 2026 citrustiara. SPDX-License-Identifier: Apache-2.0
// Adapted from RoadForge; standalone validation and size arithmetic hardened.
// RFPACK01 — a packed typed-array interchange format.
// One buffer: 8-byte magic, LE u32 JSON header length, UTF-8 JSON header
// {"sections":[{name,elem,count,offset}...],"meta":{...}}, then raw
// little-endian sections in an area that starts at the next 8-byte boundary
// after the header; every section offset is relative to that area and itself
// 8-byte aligned. The byte layout is unchanged from the extracted format.

export type RfElem = "f32" | "f64" | "u32" | "i32" | "u8";

type RfTypedArray = Float32Array | Float64Array | Uint32Array | Int32Array | Uint8Array;

const ELEM_SIZE: Record<RfElem, number> = { f32: 4, f64: 8, u32: 4, i32: 4, u8: 1 };

function elemOf(data: RfTypedArray): RfElem {
  if (data instanceof Float32Array) return "f32";
  if (data instanceof Float64Array) return "f64";
  if (data instanceof Uint32Array) return "u32";
  if (data instanceof Int32Array) return "i32";
  return "u8";
}

export class RfPackWriter {
  private sections: { name: string; elem: RfElem; data: RfTypedArray }[] = [];
  private meta: unknown = {};

  add(name: string, data: RfTypedArray): void {
    if (this.sections.some((section) => section.name === name)) throw new Error("Duplicate section");
    this.sections.push({ name, elem: elemOf(data), data });
  }

  setMeta(meta: unknown): void {
    this.meta = meta;
  }

  finish(): Uint8Array {
    let offset = 0;
    const entries: string[] = [];
    const placed: { at: number; data: RfTypedArray }[] = [];
    for (const { name, elem, data } of this.sections) {
      const aligned = Math.ceil(offset / 8) * 8;
      entries.push(
        `{"name":${JSON.stringify(name)},"elem":"${elem}","count":${data.length},"offset":${aligned}}`,
      );
      placed.push({ at: aligned, data });
      offset = aligned + data.length * ELEM_SIZE[elem];
    }
    const header = `{"sections":[${entries.join(",")}],"meta":${JSON.stringify(this.meta)}}`;
    const headerBytes = new TextEncoder().encode(header);
    const area = Math.ceil((12 + headerBytes.length) / 8) * 8;
    const out = new Uint8Array(area + offset);
    out.set(new TextEncoder().encode("RFPACK01"), 0);
    new DataView(out.buffer).setUint32(8, headerBytes.length, true);
    out.set(headerBytes, 12);
    for (const { at, data } of placed) {
      out.set(
        new Uint8Array(data.buffer, data.byteOffset, data.byteLength),
        area + at,
      );
    }
    return out;
  }
}

export interface RfPack {
  meta: unknown;
  sections: Map<string, RfTypedArray>;
}

/**
 * Read a pack, and refuse one that does not contain what it says it does.
 *
 * Every bound below is checked against `buf`'s own extent rather than against
 * the ArrayBuffer behind it, and that is the whole point. `ArrayBuffer.slice`
 * clamps silently, so a section declared as a thousand floats in a buffer that
 * holds nine hundred used to decode into nine hundred floats and no error at
 * all — a truncated cache entry coming back as a mesh with a hole in it rather
 * than as a miss. A view over a larger buffer was worse: the missing hundred
 * were read out of whatever happened to sit next to it.
 *
 * So a pack that is short, or whose header describes sections that do not fit,
 * throws. Callers that read from a store treat that as a miss and build what
 * they were going to build anyway; callers that read a file the native core
 * just wrote are being told it is damaged.
 */
export function parseRfPack(buf: Uint8Array): RfPack {
  const magic = new TextDecoder().decode(buf.subarray(0, 8));
  if (magic !== "RFPACK01") throw new Error("not an RFPACK01 buffer");
  if (buf.byteLength < 12) throw new Error("RFPACK01 buffer ends before its header length");
  const hlen = new DataView(buf.buffer, buf.byteOffset).getUint32(8, true);
  if (12 + hlen > buf.byteLength) throw new Error("RFPACK01 header runs past the buffer");
  const header = JSON.parse(new TextDecoder().decode(buf.subarray(12, 12 + hlen))) as {
    sections: { name: string; elem: RfElem; count: number; offset: number }[];
    meta: unknown;
  };
  if (!header || !Array.isArray(header.sections)) throw new Error("RFPACK01 header has no sections");
  const area = Math.ceil((12 + hlen) / 8) * 8;
  if (area > buf.byteLength) throw new Error("RFPACK01 missing header padding");
  const sections = new Map<string, RfTypedArray>();
  for (const s of header.sections) {
    if (!s || typeof s.name !== "string" || sections.has(s.name)) throw new Error("Invalid section name");
    const size = Object.hasOwn(ELEM_SIZE, s.elem) ? ELEM_SIZE[s.elem] : undefined;
    if (!size) throw new Error(`RFPACK01 section "${s.name}" has an unknown element kind`);
    if (!Number.isSafeInteger(s.offset) || s.offset < 0 || !Number.isSafeInteger(s.count) || s.count < 0) {
      throw new Error(`RFPACK01 section "${s.name}" is not placed at a real offset`);
    }
    const bytes = s.count * size;
    if (s.offset % 8 !== 0 || !Number.isSafeInteger(bytes) ||
        !Number.isSafeInteger(area + s.offset + bytes) || area + s.offset + bytes > buf.byteLength) {
      throw new Error(`RFPACK01 section "${s.name}" runs past the buffer`);
    }
    const start = buf.byteOffset + area + s.offset;
    switch (s.elem) {
      case "f32":
        sections.set(s.name, new Float32Array(buf.buffer.slice(start, start + bytes)));
        break;
      case "f64":
        sections.set(s.name, new Float64Array(buf.buffer.slice(start, start + bytes)));
        break;
      case "u32":
        sections.set(s.name, new Uint32Array(buf.buffer.slice(start, start + bytes)));
        break;
      case "i32":
        sections.set(s.name, new Int32Array(buf.buffer.slice(start, start + bytes)));
        break;
      case "u8":
        sections.set(s.name, new Uint8Array(buf.buffer.slice(start, start + bytes)));
        break;
    }
  }
  return { meta: header.meta, sections };
}
