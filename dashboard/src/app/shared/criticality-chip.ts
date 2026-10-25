import { Component, computed, input } from '@angular/core';

/** Colored badge for an incident's criticality (HIGH / MEDIUM / LOW, see IncidentPlan.java). */
@Component({
  selector: 'app-criticality-chip',
  template: `<span class="chip" [class]="'chip ' + level()">{{ value() }}</span>`,
  styles: `
    .chip {
      display: inline-block;
      padding: 2px 10px;
      border-radius: 12px;
      font-size: 12px;
      font-weight: 600;
      letter-spacing: 0.04em;
      color: #fff;
      background: #757575;
    }
    .high { background: #c62828; }
    .medium { background: #ef6c00; }
    .low { background: #2e7d32; }
  `,
})
export class CriticalityChip {
  readonly value = input.required<string>();
  protected readonly level = computed(() => this.value().toLowerCase());
}
