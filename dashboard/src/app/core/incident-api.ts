import { Injectable, inject } from '@angular/core';
import { Observable, map } from 'rxjs';

import { GraphqlClient, INCIDENTS_ENDPOINT } from './graphql-client';

// Mirrors incident-api-service/src/main/resources/graphql/schema.graphqls.
export interface Incident {
  incidentId: string;
  alertId: string;
  sensorId: string;
  metricType: string | null;
  criticality: string;
  impactedObjects: string[];
  remediationSteps: string[];
  summary: string | null;
  model: string | null;
  createdAt: string;
}

export interface Metric {
  timestamp: string;
  sensorId: string;
  metricType: string;
  value: number;
  status: string;
}

const INCIDENT_FIELDS = `incidentId alertId sensorId metricType criticality impactedObjects
  remediationSteps summary model createdAt`;

@Injectable({ providedIn: 'root' })
export class IncidentApi {
  private readonly gql = inject(GraphqlClient);

  incidents(limit = 50): Observable<Incident[]> {
    return this.gql
      .query<{ incidents: Incident[] }>(
        INCIDENTS_ENDPOINT,
        `query($limit: Int) { incidents(limit: $limit) { ${INCIDENT_FIELDS} } }`,
        { limit },
      )
      .pipe(map((d) => d.incidents));
  }

  incident(id: string): Observable<Incident | null> {
    return this.gql
      .query<{ incident: Incident | null }>(
        INCIDENTS_ENDPOINT,
        `query($id: ID!) { incident(id: $id) { ${INCIDENT_FIELDS} } }`,
        { id },
      )
      .pipe(map((d) => d.incident));
  }

  incidentsBySensor(sensorId: string, limit = 50): Observable<Incident[]> {
    return this.gql
      .query<{ incidentsBySensor: Incident[] }>(
        INCIDENTS_ENDPOINT,
        `query($sensorId: ID!, $limit: Int) {
          incidentsBySensor(sensorId: $sensorId, limit: $limit) { ${INCIDENT_FIELDS} } }`,
        { sensorId, limit },
      )
      .pipe(map((d) => d.incidentsBySensor));
  }

  /** Recent readings in chronological order (the API returns most recent first). */
  recentMetrics(sensorId: string, limit = 200): Observable<Metric[]> {
    return this.gql
      .query<{ recentMetrics: Metric[] }>(
        INCIDENTS_ENDPOINT,
        `query($sensorId: ID!, $limit: Int) {
          recentMetrics(sensorId: $sensorId, limit: $limit) { timestamp sensorId metricType value status } }`,
        { sensorId, limit },
      )
      .pipe(map((d) => [...d.recentMetrics].reverse()));
  }
}
