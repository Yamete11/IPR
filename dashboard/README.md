# dashboard

Angular 21 (standalone components, signals, zoneless) + Angular Material UI over the two read-only
GraphQL APIs:

| Screen | Route | Data |
|---|---|---|
| Incidents | `/incidents` | `incidents` — table, criticality counts, auto-refresh every 10 s |
| Incident detail | `/incidents/:id` | `incident` + `recentMetrics` of its sensor — remediation steps, impacted objects |
| Topology | `/topology?focus=<id>` | `vertices`/`edges` (cytoscape graph), `dependents`/`dependencies` of the clicked node |
| Sensor readings | `/sensors/:id` | `recentMetrics` chart (chart.js) with the detector threshold, `incidentsBySensor` |

The browser only ever calls same-origin paths:

| Path | `ng serve` (`proxy.conf.json`) | Docker / Kubernetes (`nginx.conf.template`) |
|---|---|---|
| `/api/topology/graphql` | `http://localhost:8080/graphql` | `$TOPOLOGY_UPSTREAM/graphql` (default `http://topology-service:8080`) |
| `/api/incidents/graphql` | `http://localhost:8081/graphql` | `$INCIDENT_API_UPSTREAM/graphql` (default `http://incident-api-service:8081`) |

Detector thresholds drawn on the charts are copied in `src/app/core/detector-rules.ts` from
`anomaly-detector-service/src/main/resources/application.yaml` — keep them in sync.

## Commands

Node 22.12+ (Angular 21; Angular 22 would need Node 22.22+). Install with npm 11 — npm 10 (the one
bundled with Node 22) crashes resolving vitest's optional peer dependencies
(`Cannot read properties of null (reading 'edgesOut')`); the Dockerfile upgrades npm for the same reason.

```bash
npx npm@11 ci
npm start                                  # http://localhost:4200, backends on :8080 / :8081
npm run build                              # dist/dashboard/browser
npx ng test --watch=false                  # vitest
docker build -t smartcity/dashboard:dev .  # nginx-unprivileged image, port 8080, UID 101
```

Helm chart: `deploy/charts/dashboard` (part of the `smartcity` umbrella).
