import { JsonPipe } from '@angular/common';
import {
  Component,
  ElementRef,
  OnDestroy,
  afterRenderEffect,
  computed,
  inject,
  input,
  signal,
  viewChild,
} from '@angular/core';
import { rxResource, toSignal } from '@angular/core/rxjs-interop';
import { MatButtonModule } from '@angular/material/button';
import { MatCardModule } from '@angular/material/card';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatIconModule } from '@angular/material/icon';
import { MatListModule } from '@angular/material/list';
import { MatSelectModule } from '@angular/material/select';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { Router, RouterLink } from '@angular/router';
import cytoscape, { Core, ElementDefinition } from 'cytoscape';
import { catchError, forkJoin, map, of } from 'rxjs';

import { IncidentApi } from '../core/incident-api';
import { Topology, TopologyApi, Vertex } from '../core/topology-api';
import { districtOf, substationOf, substations } from './district';

const ALL = 'all';

/** Node color + shape per vertex label of the seed graph (city-topology.json). */
export const LABEL_STYLE: Record<string, { color: string; shape: cytoscape.Css.NodeShape }> = {
  Substation: { color: '#6a1b9a', shape: 'hexagon' },
  DistributionNode: { color: '#1565c0', shape: 'diamond' },
  ResidentialBuilding: { color: '#2e7d32', shape: 'round-rectangle' },
  Sensor: { color: '#ef6c00', shape: 'ellipse' },
};

const EDGE_COLOR: Record<string, string> = {
  PowerLine: '#6a1b9a',
  Pipeline: '#1565c0',
  SensorLink: '#9e9e9e',
};

@Component({
  selector: 'app-topology-view',
  imports: [
    JsonPipe,
    RouterLink,
    MatButtonModule,
    MatCardModule,
    MatFormFieldModule,
    MatIconModule,
    MatListModule,
    MatSelectModule,
    MatSlideToggleModule,
  ],
  templateUrl: './topology-view.html',
  styleUrl: './topology-view.scss',
})
export class TopologyView implements OnDestroy {
  private readonly topologyApi = inject(TopologyApi);
  private readonly incidentApi = inject(IncidentApi);
  private readonly router = inject(Router);

  /** Query param `?focus=<vertex id>` (withComponentInputBinding), e.g. from an incident page. */
  readonly focus = input<string | undefined>();

  private readonly container = viewChild.required<ElementRef<HTMLDivElement>>('graph');
  private cy?: Core;

  protected readonly legend = Object.entries(LABEL_STYLE).map(([label, s]) => ({ label, ...s }));
  protected readonly edgeLegend = Object.entries(EDGE_COLOR).map(([type, color]) => ({ type, color }));

  protected readonly topology = rxResource({ stream: () => this.topologyApi.topology() });

  /**
   * Sensors and assets with at least one incident get a red ring (the asset too, so the ring shows
   * while sensors are hidden); the graph still renders without incident-api.
   */
  private readonly incidentSensors = toSignal(
    this.incidentApi.incidents(1000).pipe(
      map((list) => new Set(list.flatMap((i) => (i.assetId ? [i.sensorId, i.assetId] : [i.sensorId])))),
      catchError(() => of(new Set<string>())),
    ),
    { initialValue: new Set<string>() },
  );

  /** District filter: one substation's subtree, or `all`. Sensors are hidden unless toggled on. */
  protected readonly district = signal<string>(ALL);
  protected readonly showSensors = signal(false);
  protected readonly districts = computed(() => {
    const topology = this.topology.value();
    return topology ? substations(topology) : [];
  });
  private readonly visible = computed(() => {
    const topology = this.topology.value();
    if (!topology) return new Set<string>();
    const district = this.district();
    const inScope = district === ALL ? null : districtOf(topology, district);
    return new Set(
      topology.vertices
        .filter((v) => (inScope ? inScope.has(v.id) : true))
        .filter((v) => this.showSensors() || v.label !== 'Sensor')
        .map((v) => v.id),
    );
  });

  protected readonly selectedId = signal<string | undefined>(undefined);
  protected readonly selected = computed(() =>
    this.topology.value()?.vertices.find((v) => v.id === this.selectedId()),
  );

  protected readonly relations = rxResource({
    params: () => this.selectedId(),
    stream: ({ params }) =>
      forkJoin({
        dependents: this.topologyApi.dependents(params),
        dependencies: this.topologyApi.dependencies(params),
      }),
  });

  /** Impact of losing the selected vertex; a failure here only hides the panel section. */
  protected readonly impact = rxResource({
    params: () => this.selectedId(),
    stream: ({ params }) => this.topologyApi.impact(params),
  });

  constructor() {
    // Build the graph once the topology arrives; re-mark incident sensors when they load.
    afterRenderEffect(() => {
      const topology = this.topology.value();
      if (topology && !this.cy) {
        this.cy = this.createGraph(topology);
        const focus = this.focus();
        if (focus) {
          this.district.set(substationOf(topology, focus) ?? ALL);
          if (topology.vertices.find((v) => v.id === focus)?.label === 'Sensor') this.showSensors.set(true);
          this.selectedId.set(focus);
        }
      }
      const sensors = this.incidentSensors();
      this.cy?.nodes().forEach((n) => {
        n.toggleClass('has-incident', sensors.has(n.id()));
      });
    });

    // Show only the chosen district (and sensors if toggled), and lay out just what is visible.
    afterRenderEffect(() => {
      const cy = this.cy;
      const visible = this.visible();
      const topology = this.topology.value();
      if (!cy || !topology) return;
      cy.batch(() => {
        cy.nodes().forEach((n) => {
          n.toggleClass('hidden', !visible.has(n.id()));
        });
      });
      const shown = cy.elements().not('.hidden').filter((el) => el.isNode() || !el.connectedNodes('.hidden').length);
      const roots = substations(topology).map((v) => v.id).filter((id) => visible.has(id));
      shown
        .layout({ name: 'breadthfirst', directed: false, padding: 24, spacingFactor: 1.2, roots, animate: false })
        .run();
      cy.fit(shown, 24);
    });

    // Highlight the selected vertex and the vertices that depend on it.
    afterRenderEffect(() => {
      const cy = this.cy;
      const id = this.selectedId();
      const rel = this.relations.value();
      if (!cy || this.topology.isLoading()) return;
      cy.elements().removeClass('selected dependent dependency faded');
      if (!id) return;
      cy.getElementById(id).addClass('selected');
      if (!rel) return;
      const dependents = new Set(rel.dependents.map((v) => v.id));
      const dependencies = new Set(rel.dependencies.map((v) => v.id));
      cy.nodes().forEach((n) => {
        if (dependents.has(n.id())) n.addClass('dependent');
        else if (dependencies.has(n.id())) n.addClass('dependency');
        else if (n.id() !== id) n.addClass('faded');
      });
    });
  }

  ngOnDestroy(): void {
    this.cy?.destroy();
  }

  protected select(id: string): void {
    this.selectedId.set(id);
    this.router.navigate([], { queryParams: { focus: id }, replaceUrl: true });
  }

  protected clear(): void {
    this.selectedId.set(undefined);
    this.router.navigate([], { queryParams: {}, replaceUrl: true });
  }

  protected displayName(v: Vertex): string {
    const name = v.properties?.['name'];
    return typeof name === 'string' ? name : v.id;
  }

  private createGraph(topology: Topology): Core {
    const elements: ElementDefinition[] = [
      ...topology.vertices.map((v) => ({
        data: { id: v.id, label: v.label, name: this.displayName(v) },
      })),
      ...topology.edges.map((e, i) => ({
        data: { id: `e${i}`, source: e.from, target: e.to, type: e.type },
      })),
    ];
    const cy = cytoscape({
      container: this.container().nativeElement,
      elements,
      // Positions come from the visibility effect, which lays out only the shown district
      // (undirected BFS from the substations, so sensors hang under the asset they watch).
      layout: { name: 'preset' },
      wheelSensitivity: 0.3,
      style: [
        {
          selector: 'node',
          style: {
            label: 'data(name)',
            'font-size': 11,
            'text-valign': 'bottom',
            'text-margin-y': 6,
            color: '#424242',
            'text-background-color': '#ffffff',
            'text-background-opacity': 0.85,
            'text-background-padding': '2px',
            width: 34,
            height: 34,
            'border-width': 2,
            'border-color': '#fff',
          },
        },
        ...Object.entries(LABEL_STYLE).map(([label, s]) => ({
          selector: `node[label = "${label}"]`,
          style: { 'background-color': s.color, shape: s.shape },
        })),
        {
          selector: 'edge',
          style: {
            width: 2,
            'curve-style': 'bezier',
            'target-arrow-shape': 'triangle',
            'line-color': '#9e9e9e',
            'target-arrow-color': '#9e9e9e',
          },
        },
        ...Object.entries(EDGE_COLOR).map(([type, color]) => ({
          selector: `edge[type = "${type}"]`,
          style: {
            'line-color': color,
            'target-arrow-color': color,
            'line-style': type === 'SensorLink' ? 'dashed' : 'solid',
          } as cytoscape.Css.Edge,
        })),
        { selector: 'node.has-incident', style: { 'border-color': '#c62828', 'border-width': 4 } },
        { selector: 'node.selected', style: { 'border-color': '#212121', 'border-width': 5, width: 44, height: 44 } },
        { selector: 'node.dependent', style: { 'overlay-color': '#c62828', 'overlay-opacity': 0.25, 'overlay-padding': 6 } },
        { selector: 'node.dependency', style: { 'overlay-color': '#1565c0', 'overlay-opacity': 0.2, 'overlay-padding': 6 } },
        { selector: 'node.faded', style: { opacity: 0.35 } },
        { selector: 'node.hidden', style: { display: 'none' } },
      ],
    });
    cy.on('tap', 'node', (evt) => this.select(evt.target.id()));
    cy.on('tap', (evt) => {
      if (evt.target === cy) this.clear();
    });
    return cy;
  }
}
