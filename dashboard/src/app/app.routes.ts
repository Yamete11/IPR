import { Routes } from '@angular/router';

// Lazy routes keep cytoscape / chart.js out of the initial bundle.
export const routes: Routes = [
  { path: '', pathMatch: 'full', redirectTo: 'incidents' },
  {
    path: 'incidents',
    title: 'Incidents · Smart City',
    loadComponent: () => import('./incidents/incident-list').then((m) => m.IncidentList),
  },
  {
    path: 'incidents/:id',
    title: 'Incident · Smart City',
    loadComponent: () => import('./incidents/incident-detail').then((m) => m.IncidentDetail),
  },
  {
    path: 'topology',
    title: 'Topology · Smart City',
    loadComponent: () => import('./topology/topology-view').then((m) => m.TopologyView),
  },
  {
    path: 'sensors',
    title: 'Sensors · Smart City',
    loadComponent: () => import('./metrics/sensor-metrics').then((m) => m.SensorMetrics),
  },
  {
    path: 'sensors/:id',
    title: 'Sensor · Smart City',
    loadComponent: () => import('./metrics/sensor-metrics').then((m) => m.SensorMetrics),
  },
  { path: '**', redirectTo: 'incidents' },
];
