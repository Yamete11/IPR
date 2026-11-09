import { Topology } from '../core/topology-api';
import { districtOf, substationOf, substations } from './district';

const vertex = (id: string, label: string) => ({ id, label, properties: null });
const edge = (type: string, from: string, to: string) => ({ type, from, to, properties: null });

// The hand seed plus one generated district: sub-d00 → node-d00-0 → bld-d00-0-0.
const TOPOLOGY: Topology = {
  vertices: [
    vertex('sub-1', 'Substation'),
    vertex('node-5', 'DistributionNode'),
    vertex('bld-12', 'ResidentialBuilding'),
    vertex('s-00042', 'Sensor'),
    vertex('s-00311', 'Sensor'),
    vertex('sub-d00', 'Substation'),
    vertex('node-d00-0', 'DistributionNode'),
    vertex('bld-d00-0-0', 'ResidentialBuilding'),
    vertex('s-01000', 'Sensor'),
  ],
  edges: [
    edge('PowerLine', 'sub-1', 'node-5'),
    edge('Pipeline', 'node-5', 'bld-12'),
    edge('SensorLink', 's-00042', 'node-5'),
    edge('SensorLink', 's-00311', 'sub-1'),
    edge('PowerLine', 'sub-d00', 'node-d00-0'),
    edge('Pipeline', 'node-d00-0', 'bld-d00-0-0'),
    edge('SensorLink', 's-01000', 'bld-d00-0-0'),
  ],
};

describe('districts', () => {
  it('lists substations sorted by id', () => {
    expect(substations(TOPOLOGY).map((v) => v.id)).toEqual(['sub-1', 'sub-d00']);
  });

  it('collects a substation, everything it feeds and the sensors on them', () => {
    expect([...districtOf(TOPOLOGY, 'sub-1')].sort()).toEqual(['bld-12', 'node-5', 's-00042', 's-00311', 'sub-1']);
    expect(districtOf(TOPOLOGY, 'sub-d00').has('node-5')).toBe(false);
  });

  it('finds the substation of a sensor, an asset or a substation', () => {
    expect(substationOf(TOPOLOGY, 's-01000')).toBe('sub-d00');
    expect(substationOf(TOPOLOGY, 'bld-12')).toBe('sub-1');
    expect(substationOf(TOPOLOGY, 's-00311')).toBe('sub-1');
    expect(substationOf(TOPOLOGY, 'sub-d00')).toBe('sub-d00');
    expect(substationOf(TOPOLOGY, 'nowhere')).toBeUndefined();
  });
});
