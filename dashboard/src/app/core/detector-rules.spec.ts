import { ruleFor, ruleLabel, violates } from './detector-rules';

describe('detector rules', () => {
  it('derives the same rule label as the anomaly detector', () => {
    expect(ruleLabel(ruleFor('cable_temperature')!)).toBe('cable_temperature_max>120');
    expect(ruleLabel(ruleFor('pipe_pressure')!)).toBe('pipe_pressure_min<0.5');
  });

  it('flags readings on the failing side of the threshold', () => {
    const pressure = ruleFor('pipe_pressure')!;
    expect(violates(pressure, 0)).toBe(true);
    expect(violates(pressure, 3)).toBe(false);
    expect(violates(ruleFor('cable_temperature')!, 130)).toBe(true);
  });

  it('has no rule for unknown metrics', () => {
    expect(ruleFor('humidity')).toBeUndefined();
    expect(ruleFor(null)).toBeUndefined();
  });
});
