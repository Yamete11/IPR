import { Injectable, inject } from '@angular/core';
import { Observable, map } from 'rxjs';

import { GraphqlClient, TOPOLOGY_ENDPOINT } from './graphql-client';

// Mirrors topology-service/src/main/resources/graphql/schema.graphqls.
export interface Vertex {
  id: string;
  label: string;
  properties: Record<string, unknown> | null;
}

export interface Edge {
  type: string;
  from: string;
  to: string;
  properties: Record<string, unknown> | null;
}

export interface Topology {
  vertices: Vertex[];
  edges: Edge[];
}

export interface ImpactSummary {
  sourceId: string;
  totalObjects: number;
  apartments: number;
  byLabel: { label: string; count: number }[];
}

export interface Crew {
  crewId: string;
  name: string;
  phone: string | null;
  assets: string[];
}

export interface Impact {
  summary: ImpactSummary;
  crews: Crew[];
}

@Injectable({ providedIn: 'root' })
export class TopologyApi {
  private readonly gql = inject(GraphqlClient);

  topology(): Observable<Topology> {
    return this.gql.query<Topology>(
      TOPOLOGY_ENDPOINT,
      `{ vertices { id label properties } edges { type from to properties } }`,
    );
  }

  /** What loses service if `id` fails. */
  dependents(id: string): Observable<Vertex[]> {
    return this.gql
      .query<{ dependents: Vertex[] }>(
        TOPOLOGY_ENDPOINT,
        `query($id: ID!) { dependents(id: $id) { id label properties } }`,
        { id },
      )
      .pipe(map((d) => d.dependents));
  }

  /** Cypher aggregation over the dependents + the crews joined to them in SQL, in one request. */
  impact(id: string): Observable<Impact> {
    return this.gql
      .query<{ impactSummary: ImpactSummary; responsibleCrews: Crew[] }>(
        TOPOLOGY_ENDPOINT,
        `query($id: ID!) {
          impactSummary(id: $id) { sourceId totalObjects apartments byLabel { label count } }
          responsibleCrews(id: $id) { crewId name phone assets }
        }`,
        { id },
      )
      .pipe(map((d) => ({ summary: d.impactSummary, crews: d.responsibleCrews })));
  }

  /** What `id` relies on. */
  dependencies(id: string): Observable<Vertex[]> {
    return this.gql
      .query<{ dependencies: Vertex[] }>(
        TOPOLOGY_ENDPOINT,
        `query($id: ID!) { dependencies(id: $id) { id label properties } }`,
        { id },
      )
      .pipe(map((d) => d.dependencies));
  }
}
