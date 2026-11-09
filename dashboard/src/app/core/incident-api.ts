import { Injectable, inject } from '@angular/core';
import { Observable, map } from 'rxjs';

import { GraphqlClient, INCIDENTS_ENDPOINT } from './graphql-client';

// Mirrors incident-api-service/src/main/resources/graphql/schema.graphqls.
export interface Incident {
  incidentId: string;
  alertId: string;
  sensorId: string;
  metricType: string | null;
  /** Graph asset the sensor watches; null if the sensor is not in the topology graph. */
  assetId: string | null;
  /** Set when the responder filed this as a secondary effect of an upstream incident. */
  rootIncidentId: string | null;
  detectedAt: string | null;
  criticality: string;
  impactedObjects: string[];
  remediationSteps: string[];
  summary: string | null;
  model: string | null;
  createdAt: string;
}

export interface IncidentCrew {
  crewId: string;
  name: string;
  phone: string | null;
  assets: string[];
}

/** The detail page's incident: plus its crews, its root and what is filed under it. */
export interface IncidentDetailView extends Incident {
  crews: IncidentCrew[];
  rootIncident: Pick<Incident, 'incidentId' | 'sensorId' | 'assetId' | 'summary'> | null;
  linkedIncidents: Pick<Incident, 'incidentId' | 'sensorId' | 'metricType' | 'assetId' | 'detectedAt' | 'model'>[];
}

export interface Metric {
  timestamp: string;
  sensorId: string;
  metricType: string;
  value: number;
  status: string;
}

const INCIDENT_FIELDS = `incidentId alertId sensorId metricType assetId rootIncidentId detectedAt criticality
  impactedObjects remediationSteps summary model createdAt`;

// Nested fields are resolved per incident, so only the single-incident query asks for them.
const DETAIL_FIELDS = `${INCIDENT_FIELDS}
  crews { crewId name phone assets }
  rootIncident { incidentId sensorId assetId summary }
  linkedIncidents { incidentId sensorId metricType assetId detectedAt model }`;

@Injectable({ providedIn: 'root' })
export class IncidentApi {
  private readonly gql = inject(GraphqlClient);

  incidents(limit = 50): Observable<Incident[]> {
    return this.gql
      .query<{ incidents: Incident[] }>(
        INCIDENTS_ENDPOINT,
        `query($limit: Int!) { incidents(limit: $limit) { ${INCIDENT_FIELDS} } }`,
        { limit },
      )
      .pipe(map((d) => d.incidents));
  }

  incident(id: string): Observable<IncidentDetailView | null> {
    return this.gql
      .query<{ incident: IncidentDetailView | null }>(
        INCIDENTS_ENDPOINT,
        `query($id: ID!) { incident(id: $id) { ${DETAIL_FIELDS} } }`,
        { id },
      )
      .pipe(map((d) => d.incident));
  }

  incidentsBySensor(sensorId: string, limit = 50): Observable<Incident[]> {
    return this.gql
      .query<{ incidentsBySensor: Incident[] }>(
        INCIDENTS_ENDPOINT,
        `query($sensorId: ID!, $limit: Int!) {
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
        `query($sensorId: ID!, $limit: Int!) {
          recentMetrics(sensorId: $sensorId, limit: $limit) { timestamp sensorId metricType value status } }`,
        { sensorId, limit },
      )
      .pipe(map((d) => [...d.recentMetrics].reverse()));
  }
}
