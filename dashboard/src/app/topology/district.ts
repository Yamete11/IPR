import { Topology, Vertex } from '../core/topology-api';

/**
 * A district is one substation and everything it feeds (PowerLine / Pipeline, downstream), plus the
 * sensors linked into those assets. The generated city has 21 of them; drawing one at a time keeps
 * the topology screen readable at ~1400 vertices.
 */

const SENSOR_LINK = 'SensorLink';

/** Substations, sorted by id: the options of the district filter. */
export function substations(topology: Topology): Vertex[] {
  return topology.vertices
    .filter((v) => v.label === 'Substation')
    .sort((a, b) => a.id.localeCompare(b.id));
}

/** Ids of the substation, its downstream assets and the sensors watching any of them. */
export function districtOf(topology: Topology, substationId: string): Set<string> {
  const downstream = new Map<string, string[]>();
  for (const e of topology.edges) {
    if (e.type === SENSOR_LINK) continue;
    downstream.set(e.from, [...(downstream.get(e.from) ?? []), e.to]);
  }
  const members = new Set<string>([substationId]);
  const queue = [substationId];
  while (queue.length) {
    for (const next of downstream.get(queue.shift()!) ?? []) {
      if (!members.has(next)) {
        members.add(next);
        queue.push(next);
      }
    }
  }
  for (const e of topology.edges) {
    if (e.type === SENSOR_LINK && members.has(e.to)) members.add(e.from);
  }
  return members;
}

/** The substation whose district contains `id` (a sensor, asset or substation), if any. */
export function substationOf(topology: Topology, id: string): string | undefined {
  const labels = new Map(topology.vertices.map((v) => [v.id, v.label]));
  const upstream = new Map<string, string[]>();
  let start = id;
  for (const e of topology.edges) {
    if (e.type === SENSOR_LINK) {
      if (e.from === id) start = e.to;
      continue;
    }
    upstream.set(e.to, [...(upstream.get(e.to) ?? []), e.from]);
  }
  const seen = new Set<string>([start]);
  const queue = [start];
  while (queue.length) {
    const current = queue.shift()!;
    if (labels.get(current) === 'Substation') return current;
    for (const prev of upstream.get(current) ?? []) {
      if (!seen.has(prev)) {
        seen.add(prev);
        queue.push(prev);
      }
    }
  }
  return undefined;
}
