import { DatePipe, DecimalPipe } from '@angular/common';
import { Component, computed, inject, input } from '@angular/core';
import { rxResource, toObservable, toSignal } from '@angular/core/rxjs-interop';
import { MatButtonModule } from '@angular/material/button';
import { MatCardModule } from '@angular/material/card';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { Router, RouterLink } from '@angular/router';
import { EMPTY, catchError, map, of, switchMap } from 'rxjs';

import { IncidentApi } from '../core/incident-api';
import { TopologyApi } from '../core/topology-api';
import { ruleFor, ruleLabel } from '../core/detector-rules';
import { CriticalityChip } from '../shared/criticality-chip';
import { MetricChart } from '../shared/metric-chart';
import { poll } from '../shared/poll';

@Component({
  selector: 'app-sensor-metrics',
  imports: [
    DatePipe,
    DecimalPipe,
    RouterLink,
    MatButtonModule,
    MatCardModule,
    MatFormFieldModule,
    MatInputModule,
    MatSelectModule,
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

  /** Graph-seeded sensors; synthetic ones (--extra-sensors) can be typed in. */
  protected readonly sensors = toSignal(
    this.topologyApi.topology().pipe(
      map((t) => t.vertices.filter((v) => v.label === 'Sensor')),
      catchError(() => of([])),
    ),
    { initialValue: [] },
  );

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
