import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { firstValueFrom } from 'rxjs';

import { GraphqlClient } from './graphql-client';

describe('GraphqlClient', () => {
  let client: GraphqlClient;
  let http: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({ providers: [provideHttpClient(), provideHttpClientTesting()] });
    client = TestBed.inject(GraphqlClient);
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => http.verify());

  it('posts query + variables and unwraps data', async () => {
    const result = firstValueFrom(client.query<{ n: number }>('/gql', '{ n }', { a: 1 }));
    const req = http.expectOne('/gql');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual({ query: '{ n }', variables: { a: 1 } });
    req.flush({ data: { n: 42 } });
    expect(await result).toEqual({ n: 42 });
  });

  it('fails on GraphQL errors', async () => {
    const result = firstValueFrom(client.query('/gql', '{ bad }'));
    http.expectOne('/gql').flush({ errors: [{ message: 'boom' }, { message: 'bang' }] });
    await expect(result).rejects.toThrow('boom; bang');
  });
});
