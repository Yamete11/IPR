import {
  Component,
  ElementRef,
  OnDestroy,
  afterRenderEffect,
  input,
  viewChild,
} from '@angular/core';
import {
  CategoryScale,
  Chart,
  Filler,
  Legend,
  LineController,
  LineElement,
  LinearScale,
  PointElement,
  Tooltip,
} from 'chart.js';

import { Metric } from '../core/incident-api';
import { ruleFor, ruleLabel, violates } from '../core/detector-rules';

Chart.register(LineController, LineElement, PointElement, LinearScale, CategoryScale, Tooltip, Legend, Filler);

/** Line chart of one sensor's readings (chronological) with the detector threshold, if any. */
@Component({
  selector: 'app-metric-chart',
  template: `<div class="wrap"><canvas #canvas></canvas></div>`,
  styles: `.wrap { position: relative; height: 280px; }`,
})
export class MetricChart implements OnDestroy {
  readonly metrics = input.required<Metric[]>();

  private readonly canvas = viewChild.required<ElementRef<HTMLCanvasElement>>('canvas');
  private chart?: Chart;

  constructor() {
    afterRenderEffect(() => this.render(this.metrics()));
  }

  ngOnDestroy(): void {
    this.chart?.destroy();
  }

  private render(metrics: Metric[]): void {
    const rule = ruleFor(metrics[0]?.metricType);
    const labels = metrics.map((m) => new Date(m.timestamp).toLocaleTimeString());
    const values = metrics.map((m) => m.value);
    const datasets: Chart<'line'>['data']['datasets'] = [
      {
        label: metrics[0]?.metricType ?? 'value',
        data: values,
        borderColor: '#1565c0',
        backgroundColor: 'rgba(21, 101, 192, 0.08)',
        fill: true,
        tension: 0.25,
        pointRadius: values.map((v) => (rule && violates(rule, v) ? 4 : 0)),
        pointBackgroundColor: '#c62828',
      },
    ];
    if (rule) {
      datasets.push({
        label: `threshold ${ruleLabel(rule)}`,
        data: values.map(() => rule.threshold),
        borderColor: '#c62828',
        borderDash: [6, 4],
        borderWidth: 1.5,
        pointRadius: 0,
        fill: false,
      });
    }

    if (this.chart) {
      this.chart.data.labels = labels;
      this.chart.data.datasets = datasets;
      this.chart.update('none');
      return;
    }
    this.chart = new Chart(this.canvas().nativeElement, {
      type: 'line',
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        interaction: { mode: 'index', intersect: false },
        scales: { x: { ticks: { maxTicksLimit: 8 } } },
      },
    });
  }
}
