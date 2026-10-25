import { HttpClient } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable, map } from 'rxjs';

/** Same relative paths in dev (proxy.conf.json) and in Kubernetes (nginx.conf). */
export const INCIDENTS_ENDPOINT = '/api/incidents/graphql';
export const TOPOLOGY_ENDPOINT = '/api/topology/graphql';

interface GraphqlResponse<T> {
  data?: T;
  errors?: { message: string }[];
}

/** Minimal GraphQL-over-HTTP client: POST {query, variables}, unwrap `data`, fail on `errors`. */
@Injectable({ providedIn: 'root' })
export class GraphqlClient {
  private readonly http = inject(HttpClient);

  query<T>(endpoint: string, query: string, variables: Record<string, unknown> = {}): Observable<T> {
    return this.http.post<GraphqlResponse<T>>(endpoint, { query, variables }).pipe(
      map((res) => {
        if (res.errors?.length) {
          throw new Error(res.errors.map((e) => e.message).join('; '));
        }
        if (!res.data) {
          throw new Error('GraphQL response has no data');
        }
        return res.data;
      }),
    );
  }
}
