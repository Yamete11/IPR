import { DatePipe, DecimalPipe } from '@angular/common';
import { Component, computed, inject, input, signal } from '@angular/core';
import { rxResource, toObservable, toSignal } from '@angular/core/rxjs-interop';
import { MatAutocompleteModule } from '@angular/material/autocomplete';
import { MatButtonModule } from '@angular/material/button';
import { MatCardModule } from '@angular/material/card';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { Router, RouterLink } from '@angular/router';
import { EMPTY, catchError, map, of, switchMap } from 'rxjs';

import { IncidentApi } from '../core/incident-api';
import { TopologyApi } from '../core/topology-api';
import { ruleFor, ruleLabel } from '../core/detector-rules';
import { CriticalityChip } from '../shared/criticality-chip';
import { MetricChart } from '../shared/metric-chart';
import { poll } from '../shared/poll';

const MAX_MATCHES = 50;

@Component({
  selector: 'app-sensor-metrics',
  imports: [
    DatePipe,
    DecimalPipe,
    RouterLink,
    MatAutocompleteModule,
    MatButtonModule,
    MatCardModule,
    MatFormFieldModule,
    MatInputModule,
    CriticalityChip,
    MetricChart,
  ],
  templateUrl: './sensor-metrics.html',
  styleUrl: './sensor-metrics.scss',
})
export class SensorMetrics {
  private readonly incidentApi = inject(IncidentApi);
  private readonly topologyApi = inject(TopologyApi);
  private readonly router = inject(Router);

  /** Route param `:id` (optional). */
  readonly id = input<string | undefined>();

  /** Sensors in the graph (hand seed + generated city); ids outside it can still be typed in. */
  protected readonly sensors = toSignal(
    this.topologyApi.topology().pipe(
      map((t) => t.vertices.filter((v) => v.label === 'Sensor').sort((a, b) => a.id.localeCompare(b.id))),
      catchError(() => of([])),
    ),
    { initialValue: [] },
  );

  /** What is typed in the search box; the autocomplete shows at most MAX_MATCHES sensors containing it. */
  protected readonly query = signal('');
  protected readonly matches = computed(() => {
    const q = this.query().trim().toLowerCase();
    return this.sensors()
      .filter((s) => s.id.toLowerCase().includes(q))
      .slice(0, MAX_MATCHES);
  });

  private readonly readings = toSignal(
    toObservable(this.id).pipe(
      switchMap((id) => (id ? poll(() => this.incidentApi.recentMetrics(id, 200)) : EMPTY)),
    ),
  );
  protected readonly metrics = computed(() => this.readings()?.data ?? []);
  protected readonly error = computed(() => this.readings()?.error);

  protected readonly latest = computed(() => this.metrics().at(-1));
  protected readonly rule = computed(() => ruleFor(this.latest()?.metricType));
  protected readonly ruleLabel = ruleLabel;

  protected readonly incidents = rxResource({
    params: () => this.id(),
    stream: ({ params }) => this.incidentApi.incidentsBySensor(params, 20),
  });

  protected choose(id: string): void {
    const trimmed = id.trim();
    if (trimmed) this.router.navigate(['/sensors', trimmed]);
  }
}
