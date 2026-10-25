# Graph vs relational: the topology on Apache AGE

How the Smart City topology uses the labeled property graph model, what Cypher in AGE gives us over
plain SQL, and what it costs. Every claim here is backed by code and a test in `topology-service`.

## 1. The labeled property graph (LPG) model here

| LPG element | In this project | Example (`seed/city-topology.json`) |
|---|---|---|
| Vertex + label | `Substation`, `DistributionNode`, `ResidentialBuilding`, `Sensor` | `(:DistributionNode {id: 'node-5'})` |
| Edge + type (directed) | `PowerLine`, `Pipeline`, `SensorLink` | `(sub-1)-[:PowerLine]->(node-5)` |
| Properties (on both) | free-form key/value maps | `bld-12 {apartments: 84}`, `PowerLine {capacity_mw: 40}` |

Edges point in the direction service flows (substation → node → building; sensor → monitored asset),
so "what loses service if X fails" is "everything reachable from X", and "what X relies on" is
"everything that reaches X".

**How AGE stores it.** A graph is a PostgreSQL schema (`city_topology`). Every vertex label is a table
`(id graphid, properties agtype)` inheriting from `_ag_label_vertex`; every edge type is a table
`(id, start_id, end_id, properties)` inheriting from `_ag_label_edge`. Cypher is compiled into SQL over
these tables. Nothing is stored "natively" — that matters for §4.

## 2. Cypher features used (all in `TopologyRepository`)

| Feature | Query | GraphQL |
|---|---|---|
| Pattern matching | `MATCH (s:Sensor {id: $id})-[:SensorLink]->(a)` (ai-incident-responder `GraphRepository`) | — |
| Variable-length traversal + filtering | `MATCH (n {id: $id})-[*1..10]->(d) WHERE label(n) <> 'Sensor' AND label(d) <> 'Sensor' RETURN DISTINCT ...` | `dependents`, `dependencies` |
| Aggregation (implicit GROUP BY) | `... WITH DISTINCT d RETURN label(d), count(d), sum(coalesce(d.apartments, 0))` | `impactSummary` |
| Path finding | `MATCH p = (a {id: $from})-[*1..10]->(b {id: $to}) RETURN nodes(p), length(p) ORDER BY length(p) LIMIT n` | `paths` |

Try them in GraphiQL (`topology-service` with `--spring.profiles.active=dev`, <http://localhost:8080/graphiql>):

```graphql
{ impactSummary(id: "node-5") { totalObjects apartments byLabel { label count } } }
# → 2 objects, 204 apartments, ResidentialBuilding: 2
{ paths(from: "sub-1", to: "bld-12") { length nodes { id label } } }
# → length 2: sub-1 → node-5 → bld-12
{ responsibleCrews(id: "node-5") { crewId name assets } }
# → crew-grid [node-5], crew-north [bld-12, bld-19]
```

AGE 1.6.0 dialect limits we hit (verified, not assumed): no `shortestPath()`, no relationship-type
union in a variable-length pattern (`[:PowerLine|Pipeline*]`), no `all()` predicate over path edges, and
list comprehensions over `nodes(p)` fail ("could not find properties for x") — hence `nodes(p)` is
returned whole and parsed (`AgtypeParser.parseVertexList`, which strips the `::vertex` annotations).

## 3. AGE inside PostgreSQL: hybrid SQL + Cypher

`cypher()` is a set-returning function, so a Cypher result is just another row source in SQL:

```sql
SELECT c.crew_id, c.name, c.phone, array_agg(a.asset_id ORDER BY a.asset_id) AS assets
FROM cypher('city_topology', $$
    MATCH (n {id: $id})-[*0..10]->(d)
    WHERE label(n) <> 'Sensor' AND label(d) <> 'Sensor'
    RETURN DISTINCT d.id
$$, ?::agtype) AS g(asset_id agtype)                         -- graph side, parameter bound as agtype
JOIN public.crew_assignments a ON a.asset_id = g.asset_id::text  -- agtype string → text
JOIN public.repair_crews c     ON c.crew_id = a.crew_id          -- ordinary relational tables
GROUP BY c.crew_id, c.name, c.phone
```

(`TopologyRepository.findResponsibleCrews`; tables from `topology-service/src/main/resources/schema.sql`
and `data.sql`.) Other integration points: parameters are always bound as one `agtype` map
(`?::agtype`, never string-interpolated), `search_path = ag_catalog, ...` is set per connection by
Hikari, and the benchmark below runs a plain `WITH RECURSIVE` straight over AGE's own label tables.

## 4. Indexing graph properties

`AgeClient.ensurePropertyIndex` creates a GIN index on `properties` of every vertex label table after
seeding. AGE compiles a property map in a pattern, `(n:DistributionNode {id: 'node-5'})`, into agtype
containment, which GIN supports — proven by `TopologyRepositoryIntegrationTest.propertyIndexIsUsed`:

```
Bitmap Heap Scan on "DistributionNode" n
  Recheck Cond: (properties @> '{"id": "node-5"}'::agtype)
  ->  Bitmap Index Scan on "city_topology_DistributionNode_props_gin"
        Index Cond: (properties @> '{"id": "node-5"}'::agtype)
```

Without a label, `(n {id: ...})` becomes an `Append` over every label table, each using its own index.
**AGE creates no index on edge tables' `start_id` / `end_id`** — every hop is then a scan; the benchmark
adds B-tree indexes on both.

## 5. Where graph traversal beats JOINs — and what AGE actually does

The case for a graph model is a variable-depth walk: "everything downstream of X" is one `MATCH` with
`[*1..10]`, whereas SQL needs a recursive CTE per relationship pattern, and plain JOINs only work for a
fixed, known depth. Native graph databases (e.g. Neo4j) also make it *fast*: each vertex stores direct
pointers to its neighbours (index-free adjacency), so a hop costs O(1) regardless of graph size.

AGE is not native: §1 shows edges are rows, so a hop is a join, the same as in SQL. To measure what that
means, `GraphVsRelationalBenchmarkTest` (`./gradlew graphBenchmark`) loads one synthetic topology —
a complete tree, branching 3, depth 8: 9841 vertices / 9840 edges — into AGE **and** a plain edge
table, checks every variant returns identical ids, and times them.

Run on 2026-10-25 (Docker Desktop, `apache/age:release_PG16_1.6.0`, 16 GB host under other load;
median of 9 runs after 3 warm-ups / server `Execution Time` from `EXPLAIN ANALYZE`, ms):

| Query | Rows | Cypher (AGE) | SQL over AGE tables | SQL over edge table | Cypher ÷ SQL |
|---|---:|---:|---:|---:|---:|
| all dependents of the root `v` (depth 8) | 9840 | 8046 / 9936 | 32.4 / 31.4 | 28.2 / 25.7 | 286× |
| all dependents of `v.0.0.0.0` (depth 4) | 120 | 168 / 214 | 3.39 / 1.54 | 1.04 / 0.30 | 162× |
| all dependents of `v.0.0.0.0.0.0.0` (depth 1) | 3 | 66.8 / 66.5 | 2.68 / 1.24 | 0.70 / 0.03 | 95× |
| fixed two hops below `v.0` | 9 | 2.70 / 1.24 | — | 0.69 / 0.02 | 4× |

What it shows:

1. **AGE's storage is not the problem.** A `WITH RECURSIVE` over AGE's own `_ag_label_edge` tables is
   within ~15 % of the plain edge table on the big walk (32 vs 28 ms).
2. **AGE's variable-length-path executor is.** The same walk in Cypher is 95–286× slower, and even a
   3-row answer costs ~67 ms: the VLE operator builds an in-memory context of the graph's edges and
   enumerates every path before `DISTINCT` collapses them, so its cost tracks the size of the graph
   and the number of paths, not the size of the answer.
3. **Fixed-length patterns are fine.** A two-hop `MATCH` compiles to ordinary joins and is within a few ms
   of hand-written SQL.

So on PostgreSQL + AGE the graph model wins on **expressiveness** (one readable `MATCH` instead of a
recursive CTE; aggregation and paths on the same pattern), not on raw speed for deep traversals. For
this project's topology (9 seed vertices, depth ≤ 3) Cypher is the right trade-off; at city scale
(~10⁴–10⁵ assets) the dependency walk would move to a recursive CTE over AGE's tables (the
"SQL over AGE tables" column — same data, no second copy) or to a native graph store.

Reproduce: `cd topology-service && ./gradlew graphBenchmark` (needs Docker; ~3 min, ~35 s of it loading
the graph). The run writes `build/reports/graph-vs-sql.md`.
