import { Observable, catchError, map, of, switchMap, timer } from 'rxjs';

/** Dashboard refresh period for the polling screens. */
export const REFRESH_MS = 10_000;

export type PollResult<T> = { data: T; error?: undefined } | { data?: undefined; error: string };

/** Runs `load` now and every `periodMs`; a failed load yields `{error}` instead of ending the stream. */
export function poll<T>(load: () => Observable<T>, periodMs = REFRESH_MS): Observable<PollResult<T>> {
  return timer(0, periodMs).pipe(
    switchMap(() =>
      load().pipe(
        map((data) => ({ data })),
        catchError((e: unknown) => of({ error: e instanceof Error ? e.message : String(e) })),
      ),
    ),
  );
}
