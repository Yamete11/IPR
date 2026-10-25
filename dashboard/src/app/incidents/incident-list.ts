import { DatePipe } from '@angular/common';
import { Component, computed, inject, signal } from '@angular/core';
import { takeUntilDestroyed, toObservable } from '@angular/core/rxjs-interop';
import { MatCardModule } from '@angular/material/card';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { MatTableModule } from '@angular/material/table';
import { Router, RouterLink } from '@angular/router';
import { EMPTY, switchMap } from 'rxjs';

import { Incident, IncidentApi } from '../core/incident-api';
import { CriticalityChip } from '../shared/criticality-chip';
import { poll } from '../shared/poll';

@Component({
  selector: 'app-incident-list',
  imports: [DatePipe, RouterLink, MatCardModule, MatSlideToggleModule, MatTableModule, CriticalityChip],
  templateUrl: './incident-list.html',
  styleUrl: './incident-list.scss',
})
export class IncidentList {
  private readonly api = inject(IncidentApi);
  private readonly router = inject(Router);

  protected readonly columns = ['createdAt', 'criticality', 'sensorId', 'metricType', 'summary', 'impacted'];
  protected readonly autoRefresh = signal(true);
  protected readonly incidents = signal<Incident[]>([]);
  protected readonly error = signal<string | undefined>(undefined);
  protected readonly loaded = signal(false);

  protected readonly counts = computed(() => {
    const rows = this.incidents();
    return {
      total: rows.length,
      high: rows.filter((i) => i.criticality === 'HIGH').length,
      sensors: new Set(rows.map((i) => i.sensorId)).size,
    };
  });

  constructor() {
    // Turning auto-refresh off keeps the last rows on screen and stops polling.
    toObservable(this.autoRefresh)
      .pipe(
        switchMap((on) => (on ? poll(() => this.api.incidents(100)) : EMPTY)),
        takeUntilDestroyed(),
      )
      .subscribe((r) => {
        this.loaded.set(true);
        this.error.set(r.error);
        if (r.data) this.incidents.set(r.data);
      });
  }

  protected open(id: string): void {
    this.router.navigate(['/incidents', id]);
  }
}
