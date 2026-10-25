import { DatePipe } from '@angular/common';
import { Component, inject, input } from '@angular/core';
import { rxResource } from '@angular/core/rxjs-interop';
import { MatButtonModule } from '@angular/material/button';
import { MatCardModule } from '@angular/material/card';
import { MatChipsModule } from '@angular/material/chips';
import { MatIconModule } from '@angular/material/icon';
import { RouterLink } from '@angular/router';

import { IncidentApi } from '../core/incident-api';
import { CriticalityChip } from '../shared/criticality-chip';
import { MetricChart } from '../shared/metric-chart';

@Component({
  selector: 'app-incident-detail',
  imports: [
    DatePipe,
    RouterLink,
    MatButtonModule,
    MatCardModule,
    MatChipsModule,
    MatIconModule,
    CriticalityChip,
    MetricChart,
  ],
  templateUrl: './incident-detail.html',
  styleUrl: './incident-detail.scss',
})
export class IncidentDetail {
  private readonly api = inject(IncidentApi);

  /** Route param `:id` (withComponentInputBinding). */
  readonly id = input.required<string>();

  protected readonly incident = rxResource({
    params: () => this.id(),
    stream: ({ params }) => this.api.incident(params),
  });

  protected readonly metrics = rxResource({
    params: () => this.incident.value()?.sensorId,
    stream: ({ params }) => this.api.recentMetrics(params, 200),
  });
}
