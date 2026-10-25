/**
 * Threshold rules of the anomaly detector, copied from
 * anomaly-detector-service/src/main/resources/application.yaml (`anomaly.rules[]`) — no API exposes
 * them. Keep the two in sync; they are only used to draw threshold lines on the charts.
 */
export interface DetectorRule {
  metricType: string;
  aggregate: 'MIN' | 'MAX';
  comparison: 'BELOW' | 'ABOVE';
  threshold: number;
}

export const DETECTOR_RULES: readonly DetectorRule[] = [
  { metricType: 'pipe_pressure', aggregate: 'MIN', comparison: 'BELOW', threshold: 0.5 },
  { metricType: 'cable_temperature', aggregate: 'MAX', comparison: 'ABOVE', threshold: 120 },
  { metricType: 'substation_voltage', aggregate: 'MIN', comparison: 'BELOW', threshold: 50 },
];

export function ruleFor(metricType: string | null | undefined): DetectorRule | undefined {
  return DETECTOR_RULES.find((r) => r.metricType === metricType);
}

/** The detector's alert label for a rule, e.g. `cable_temperature_max>120`. */
export function ruleLabel(rule: DetectorRule): string {
  const op = rule.comparison === 'BELOW' ? '<' : '>';
  return `${rule.metricType}_${rule.aggregate.toLowerCase()}${op}${rule.threshold}`;
}

/** Whether a single reading is on the failing side of the rule's threshold. */
export function violates(rule: DetectorRule, value: number): boolean {
  return rule.comparison === 'BELOW' ? value < rule.threshold : value > rule.threshold;
}
