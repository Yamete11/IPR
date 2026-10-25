package inno.ipr.topology_service.benchmark;

import com.zaxxer.hikari.HikariDataSource;
import inno.ipr.topology_service.age.AgeClient;
import inno.ipr.topology_service.support.AgeContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The same dependency questions answered two ways on one synthetic topology: as Cypher over an AGE
 * graph, and as SQL over a plain edge table ({@code WITH RECURSIVE} for the variable-depth walk,
 * two JOINs for the fixed two-hop pattern). Both sides must return the same ids; the report records
 * median wall-clock time and PostgreSQL's own execution time.
 *
 * <p>Topology: a complete tree with branching {@value #BRANCHING} and depth {@value #DEPTH} — a
 * substation at the root, distribution nodes on the inner levels, buildings on the leaves
 * (≈10k vertices). Run with {@code ./gradlew graphBenchmark}; excluded from {@code build}.
 * Writes {@code build/reports/graph-vs-sql.md}.
 *
 * <p>{@code load()} builds the vertices level by level, with ids encoding the tree position
 * ({@code v}, {@code v.0}, {@code v.0.2}, ...). It also creates B-tree indexes on
 * {@code start_id}/{@code end_id} of the AGE edge label tables: AGE indexes neither, so without them
 * every hop of both AGE-backed variants would be a scan, while the plain edge table has its primary
 * key on {@code (src, dst)}.
 *
 * <p>{@code cypherVersusSql()} compares a variable-depth walk (everything downstream, from three
 * depths of the tree) and a fixed two-hop pattern (node → node → anything), for which JOINs are the
 * natural relational form. {@code compare(String, Map)} runs every variant, asserts they all return
 * the same ids, and times each; {@code executionMillis(Query)} reads PostgreSQL's "Execution Time"
 * from {@code EXPLAIN ANALYZE} (server side, no network / JDBC).
 *
 * <p>{@code ageTablesCte(String)} is the same recursive CTE, but over the tables AGE itself stores
 * the graph in: every edge label is a child of
 * {@code <graph>._ag_label_edge(id, start_id, end_id, properties)} and every vertex label a child of
 * {@code _ag_label_vertex(id, properties)}. It separates AGE's storage from its variable-length-path
 * executor. The business id is read with {@code agtype_access_operator}.
 */
@Tag("benchmark")
@Testcontainers
@EnabledIf("inno.ipr.topology_service.support.AgeContainer#dockerAvailable")
class GraphVsRelationalBenchmarkTest {

    private static final String GRAPH = "bench_topology";
    private static final int BRANCHING = 3;
    private static final int DEPTH = 8;
    private static final int MAX_DEPTH = 10;
    private static final int WARMUP = 3;
    private static final int RUNS = 9;
    private static final int BATCH = 500;

    @Container
    private static final PostgreSQLContainer AGE = AgeContainer.create();

    private static final JsonMapper MAPPER = JsonMapper.builder().build();
    private static HikariDataSource dataSource;
    private static JdbcTemplate jdbc;
    private static int vertexCount;
    private static long loadMillis;

    @BeforeAll
    static void load() {
        dataSource = AgeContainer.dataSource(AGE);
        jdbc = new JdbcTemplate(dataSource);
        long start = System.nanoTime();

        List<List<String>> levels = new ArrayList<>();
        levels.add(List.of("v"));
        for (int d = 1; d <= DEPTH; d++) {
            List<String> level = new ArrayList<>();
            for (String parent : levels.get(d - 1)) {
                for (int c = 0; c < BRANCHING; c++) {
                    level.add(parent + "." + c);
                }
            }
            levels.add(level);
        }
        vertexCount = levels.stream().mapToInt(List::size).sum();

        AgeClient age = new AgeClient(jdbc);
        age.ensureGraph(GRAPH);
        jdbc.execute("CREATE TABLE public.bench_vertex (id TEXT PRIMARY KEY, label TEXT NOT NULL)");
        jdbc.execute("CREATE TABLE public.bench_edge (src TEXT NOT NULL, dst TEXT NOT NULL, PRIMARY KEY (src, dst))");

        for (int d = 0; d <= DEPTH; d++) {
            String label = labelAt(d);
            for (List<String> chunk : chunks(levels.get(d))) {
                age.execute(GRAPH, "UNWIND $rows AS r CREATE (:" + label + " {id: r})", params(chunk));
                jdbc.batchUpdate("INSERT INTO public.bench_vertex (id, label) VALUES (?, ?)",
                        chunk.stream().map(id -> new Object[]{id, label}).toList());
            }
            age.ensurePropertyIndex(GRAPH, label);
        }
        for (int d = 1; d <= DEPTH; d++) {
            String parentLabel = labelAt(d - 1);
            String childLabel = labelAt(d);
            String edgeType = d == 1 ? "PowerLine" : "Pipeline";
            List<Map<String, String>> edges = levels.get(d).stream()
                    .map(id -> Map.of("s", id.substring(0, id.lastIndexOf('.')), "d", id)).toList();
            for (List<Map<String, String>> chunk : chunks(edges)) {
                age.execute(GRAPH, "UNWIND $rows AS r MATCH (a:" + parentLabel + " {id: r.s}), (b:" + childLabel
                        + " {id: r.d}) CREATE (a)-[:" + edgeType + "]->(b)", params(chunk));
                jdbc.batchUpdate("INSERT INTO public.bench_edge (src, dst) VALUES (?, ?)",
                        chunk.stream().map(e -> new Object[]{e.get("s"), e.get("d")}).toList());
            }
        }
        for (String edgeType : List.of("PowerLine", "Pipeline")) {
            age.ensureEdgeIndexes(GRAPH, edgeType);
        }
        jdbc.execute("ANALYZE");
        loadMillis = (System.nanoTime() - start) / 1_000_000;
    }

    @AfterAll
    static void close() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @Test
    void cypherVersusSql() throws Exception {
        List<Result> results = new ArrayList<>();
        for (String start : List.of("v", "v.0.0.0.0", "v.0.0.0.0.0.0.0")) {
            results.add(compare("all dependents of `" + start + "`", Map.of(
                    CYPHER, cypherDependents(start),
                    AGE_CTE, ageTablesCte(start),
                    PLAIN_SQL, recursiveCte(start))));
        }
        results.add(compare("two hops below `v.0`", Map.of(
                CYPHER, cypherTwoHops("v.0"),
                PLAIN_SQL, joinTwoHops("v.0"))));

        String report = report(results);
        Path out = Path.of("build", "reports", "graph-vs-sql.md");
        Files.createDirectories(out.getParent());
        Files.writeString(out, report);
        System.out.println(report);
    }

    private static final String CYPHER = "Cypher (AGE)";
    private static final String AGE_CTE = "SQL over AGE tables";
    private static final String PLAIN_SQL = "SQL over edge table";
    private static final List<String> VARIANTS = List.of(CYPHER, AGE_CTE, PLAIN_SQL);

    private record Query(String text, List<Object> args) {
    }

    private record Timing(double medianMs, double execMs) {
    }

    private record Result(String name, int rows, Map<String, Timing> timings) {
    }

    private Result compare(String name, Map<String, Query> variants) {
        Set<String> expected = new HashSet<>(ids(variants.get(PLAIN_SQL)));
        assertThat(expected).as(name).isNotEmpty();
        Map<String, Timing> timings = new LinkedHashMap<>();
        for (String variant : VARIANTS) {
            Query q = variants.get(variant);
            if (q == null) {
                continue;
            }
            assertThat(new HashSet<>(ids(q))).as(name + " / " + variant).isEqualTo(expected);
            timings.put(variant, new Timing(medianMillis(() -> ids(q)), executionMillis(q)));
        }
        return new Result(name, expected.size(), timings);
    }

    private static Query cypherDependents(String id) {
        return new Query("""
                SELECT * FROM cypher('%s', $$
                    MATCH (n {id: $id})-[*1..%d]->(d)
                    RETURN DISTINCT d.id
                $$, ?::agtype) AS (id agtype)""".formatted(GRAPH, MAX_DEPTH), List.of(params(Map.of("id", id))));
    }

    private static Query recursiveCte(String id) {
        return new Query("""
                WITH RECURSIVE walk (id, depth) AS (
                    SELECT dst, 1 FROM public.bench_edge WHERE src = ?
                    UNION ALL
                    SELECT e.dst, w.depth + 1
                    FROM walk w JOIN public.bench_edge e ON e.src = w.id
                    WHERE w.depth < %d
                )
                SELECT DISTINCT id FROM walk""".formatted(MAX_DEPTH), List.of(id));
    }

    private static Query ageTablesCte(String id) {
        return new Query("""
                WITH RECURSIVE walk (gid, depth) AS (
                    SELECT e.end_id, 1
                    FROM %1$s._ag_label_edge e
                    WHERE e.start_id IN (SELECT v.id FROM %1$s._ag_label_vertex v WHERE v.properties @> ?::agtype)
                    UNION ALL
                    SELECT e.end_id, w.depth + 1
                    FROM walk w JOIN %1$s._ag_label_edge e ON e.start_id = w.gid
                    WHERE w.depth < %2$d
                )
                SELECT DISTINCT agtype_access_operator(v.properties, '"id"'::agtype)::text
                FROM walk w JOIN %1$s._ag_label_vertex v ON v.id = w.gid""".formatted(GRAPH, MAX_DEPTH),
                List.of(params(Map.of("id", id))));
    }

    private static Query cypherTwoHops(String id) {
        return new Query("""
                SELECT * FROM cypher('%s', $$
                    MATCH (n {id: $id})-[]->()-[]->(d)
                    RETURN DISTINCT d.id
                $$, ?::agtype) AS (id agtype)""".formatted(GRAPH), List.of(params(Map.of("id", id))));
    }

    private static Query joinTwoHops(String id) {
        return new Query("""
                SELECT DISTINCT e2.dst AS id
                FROM public.bench_edge e1
                JOIN public.bench_edge e2 ON e2.src = e1.dst
                WHERE e1.src = ?""", List.of(id));
    }

    private List<String> ids(Query q) {
        return jdbc.query(q.text(), ps -> bind(ps, q.args()), (rs, n) -> {
            String raw = rs.getString(1);
            return raw.startsWith("\"") ? raw.substring(1, raw.length() - 1) : raw;
        });
    }

    private double medianMillis(Supplier<List<String>> run) {
        for (int i = 0; i < WARMUP; i++) {
            run.get();
        }
        double[] times = new double[RUNS];
        for (int i = 0; i < RUNS; i++) {
            long t = System.nanoTime();
            run.get();
            times[i] = (System.nanoTime() - t) / 1e6;
        }
        Arrays.sort(times);
        return times[RUNS / 2];
    }

    private double executionMillis(Query q) {
        List<String> plan = jdbc.query("EXPLAIN (ANALYZE, TIMING OFF) " + q.text(),
                ps -> bind(ps, q.args()), (rs, n) -> rs.getString(1));
        return plan.stream().filter(l -> l.startsWith("Execution Time:"))
                .map(l -> Double.parseDouble(l.replaceAll("[^0-9.]", "")))
                .findFirst().orElseThrow();
    }

    private static void bind(java.sql.PreparedStatement ps, List<Object> args) throws java.sql.SQLException {
        for (int i = 0; i < args.size(); i++) {
            Object arg = args.get(i);
            if (arg instanceof AgtypeParam p) {
                ps.setObject(i + 1, p.json(), Types.OTHER);
            } else {
                ps.setObject(i + 1, arg);
            }
        }
    }

    private record AgtypeParam(String json) {
    }

    private static AgtypeParam params(Map<String, Object> map) {
        return new AgtypeParam(MAPPER.writeValueAsString(map));
    }

    private static String params(List<?> rows) {
        return MAPPER.writeValueAsString(Map.of("rows", rows));
    }

    private static String labelAt(int depth) {
        return depth == 0 ? "Substation" : depth == DEPTH ? "ResidentialBuilding" : "DistributionNode";
    }

    private static <T> List<List<T>> chunks(List<T> list) {
        List<List<T>> out = new ArrayList<>();
        for (int i = 0; i < list.size(); i += BATCH) {
            out.add(list.subList(i, Math.min(i + BATCH, list.size())));
        }
        return out;
    }

    private String report(List<Result> results) {
        Map<String, String> env = new LinkedHashMap<>();
        env.put("image", "apache/age:release_PG16_1.6.0 (Testcontainers)");
        env.put("graph", "complete tree, branching " + BRANCHING + ", depth " + DEPTH + ", "
                + vertexCount + " vertices / " + (vertexCount - 1) + " edges");
        env.put("load", loadMillis + " ms (AGE UNWIND batches + relational batch inserts)");
        env.put("timing", "median of " + RUNS + " runs after " + WARMUP + " warm-up; exec = EXPLAIN ANALYZE Execution Time");

        StringBuilder sb = new StringBuilder("# Graph (AGE Cypher) vs relational (SQL) — benchmark run\n\n");
        env.forEach((k, v) -> sb.append("- **").append(k).append("**: ").append(v).append('\n'));
        sb.append("\nMedian wall-clock / server execution time, ms (lower is better). "
                + "\"Cypher ÷ SQL\" = Cypher median over the plain-SQL median.\n\n");
        sb.append("| Query | Rows |");
        VARIANTS.forEach(v -> sb.append(' ').append(v).append(" |"));
        sb.append(" Cypher ÷ SQL |\n|---|---:|");
        VARIANTS.forEach(v -> sb.append("---:|"));
        sb.append("---:|\n");
        for (Result r : results) {
            sb.append("| ").append(r.name()).append(" | ").append(r.rows()).append(" |");
            for (String v : VARIANTS) {
                Timing t = r.timings().get(v);
                sb.append(' ').append(t == null ? "—" : fmt(t.medianMs()) + " / " + fmt(t.execMs())).append(" |");
            }
            double ratio = r.timings().get(CYPHER).medianMs() / r.timings().get(PLAIN_SQL).medianMs();
            sb.append(' ').append(String.format(java.util.Locale.ROOT, "%.0f×", ratio)).append(" |\n");
        }
        sb.append("\n- **").append(CYPHER).append("**: `MATCH (n {id: $id})-[*1..10]->(d) RETURN DISTINCT d.id`; "
                + "two-hop: `MATCH (n {id: $id})-[]->()-[]->(d)`. GIN index on every vertex label's `properties`.\n");
        sb.append("- **").append(AGE_CTE).append("**: `WITH RECURSIVE` directly over AGE's `_ag_label_edge` / "
                + "`_ag_label_vertex` tables (same data, no Cypher executor).\n");
        sb.append("- **").append(PLAIN_SQL).append("**: `WITH RECURSIVE` over `bench_edge(src, dst)`; two-hop: two JOINs.\n");
        sb.append("- B-tree indexes on `start_id`/`end_id` of every AGE edge label table "
                + "(AGE creates none); PK `(src, dst)` on `bench_edge`.\n");
        return sb.toString();
    }

    private static String fmt(double v) {
        return String.format(java.util.Locale.ROOT, "%.2f", v);
    }
}
