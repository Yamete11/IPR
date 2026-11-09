package inno.ipr.topology_service.seed;

import inno.ipr.topology_service.age.AgeClient;
import inno.ipr.topology_service.domain.EdgeType;
import inno.ipr.topology_service.domain.VertexLabel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Loads a {@link SyntheticCity} into the AGE graph and its crews into {@code public.repair_crews} /
 * {@code public.crew_assignments}, next to the hand-written seed that {@link SeedLoader} writes.
 *
 * <p>{@link #load(SyntheticCity)} runs as one transaction: either the whole city is there or none of
 * it. It first takes a transaction-scoped advisory lock, so two replicas starting together load it
 * once, then compares the city's {@link SyntheticCity#fingerprint()} with the one stored in
 * {@code public.topology_seed_state}. Equal: nothing to do (a restart costs one query, not thousands
 * of statements). Absent: load. Different: delete the previous city (every vertex with a
 * {@code district} property, every {@code crew-d*} crew) and load the new one. Returns whether it
 * wrote anything.
 *
 * <p>{@code loadVertices} creates each label's rows with {@code UNWIND $rows AS r CREATE (:Label
 * {k: r.k, ...})} in batches of {@code BATCH}, then ensures that label's GIN property index before any
 * edge is matched. {@code CREATE}, not {@code MERGE}: the fingerprint check already guarantees the
 * city is absent, and {@code MERGE} after {@code UNWIND} is not something this repo has proven on
 * AGE 1.6. Property names come from the generator and are still checked with
 * {@link AgeClient#checkIdentifier(String, String)} before interpolation.
 *
 * <p>{@code loadEdges} matches both endpoints <em>with their labels</em> (an unlabeled endpoint is an
 * {@code Append} over every label table) and ends in {@code RETURN 1}, so the returned row count must
 * equal the batch size; a shortfall means an endpoint id did not resolve and fails the load, the same
 * guard as {@code SeedLoader.seedEdges}.
 *
 * <p>{@code loadCrews} inserts crews and assignments with {@code ON CONFLICT DO NOTHING}.
 *
 * <p>{@code deleteCity} is the reload path; hand-seeded vertices have no {@code district} and
 * hand-seeded crews are not {@code crew-d*}, so neither is touched.
 */
public class SyntheticCityLoader {

    private static final Logger log = LoggerFactory.getLogger(SyntheticCityLoader.class);

    static final String SEED_NAME = "synthetic-city";
    private static final int BATCH = 500;

    private final AgeClient age;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final ObjectMapper objectMapper;
    private final String graph;

    public SyntheticCityLoader(AgeClient age, JdbcTemplate jdbc, TransactionTemplate tx,
                               ObjectMapper objectMapper, String graph) {
        this.age = age;
        this.jdbc = jdbc;
        this.tx = tx;
        this.objectMapper = objectMapper;
        this.graph = graph;
    }

    public boolean load(SyntheticCity city) {
        Boolean wrote = tx.execute(status -> {
            jdbc.queryForObject("SELECT pg_advisory_xact_lock(hashtext(?))", Object.class, graph + ":" + SEED_NAME);
            List<String> stored = jdbc.queryForList(
                    "SELECT fingerprint FROM public.topology_seed_state WHERE seed = ?", String.class, SEED_NAME);
            String fingerprint = city.fingerprint();
            if (!stored.isEmpty() && stored.getFirst().equals(fingerprint)) {
                log.info("Synthetic city already loaded ({}); nothing to do", fingerprint);
                return false;
            }
            if (!stored.isEmpty()) {
                log.info("Synthetic city changed ({} -> {}); deleting the previous one", stored.getFirst(), fingerprint);
                deleteCity();
            }
            age.ensureGraph(graph);
            loadVertices(city);
            loadEdges(city);
            loadCrews(city);
            jdbc.update("""
                    INSERT INTO public.topology_seed_state (seed, fingerprint) VALUES (?, ?)
                    ON CONFLICT (seed) DO UPDATE SET fingerprint = EXCLUDED.fingerprint, loaded_at = now()
                    """, SEED_NAME, fingerprint);
            log.info("Synthetic city loaded ({}): {} vertices, {} edges, {} crews",
                    fingerprint, city.vertexCount(), city.edgeCount(), city.crews().size());
            return true;
        });
        return Boolean.TRUE.equals(wrote);
    }

    private void loadVertices(SyntheticCity city) {
        for (SyntheticCity.VertexBatch batch : city.vertices()) {
            if (batch.rows().isEmpty()) {
                continue;
            }
            String label = batch.label().label();
            String properties = batch.rows().getFirst().keySet().stream()
                    .peek(key -> AgeClient.checkIdentifier("property", key))
                    .map(key -> key + ": r." + key)
                    .collect(Collectors.joining(", "));
            for (List<Map<String, Object>> chunk : chunks(batch.rows())) {
                age.execute(graph, "UNWIND $rows AS r CREATE (:" + label + " {" + properties + "})", rows(chunk));
            }
            age.ensurePropertyIndex(graph, label);
        }
    }

    private void loadEdges(SyntheticCity city) {
        for (SyntheticCity.EdgeBatch batch : city.edges()) {
            String type = batch.type().type();
            String cypher = "UNWIND $rows AS r MATCH (a:" + batch.from().label() + " {id: r.s}), (b:"
                    + batch.to().label() + " {id: r.d}) CREATE (a)-[:" + type + "]->(b) RETURN 1";
            for (List<Map<String, Object>> chunk : chunks(batch.rows())) {
                int created = age.execute(graph, cypher, rows(chunk));
                if (created != chunk.size()) {
                    throw new IllegalStateException("Synthetic " + type + " edges " + batch.from().label() + " -> "
                            + batch.to().label() + ": created " + created + " of " + chunk.size()
                            + "; an endpoint id did not resolve");
                }
            }
        }
        for (EdgeType type : EdgeType.values()) {
            age.ensureEdgeIndexes(graph, type.type());
        }
    }

    private void loadCrews(SyntheticCity city) {
        jdbc.batchUpdate("INSERT INTO public.repair_crews (crew_id, name, phone) VALUES (?, ?, ?) "
                        + "ON CONFLICT (crew_id) DO NOTHING",
                city.crews().stream().map(c -> new Object[]{c.crewId(), c.name(), c.phone()}).toList());
        jdbc.batchUpdate("INSERT INTO public.crew_assignments (crew_id, asset_id) VALUES (?, ?) "
                        + "ON CONFLICT (crew_id, asset_id) DO NOTHING",
                city.assignments().stream().map(a -> new Object[]{a.crewId(), a.assetId()}).toList());
    }

    private void deleteCity() {
        for (VertexLabel label : VertexLabel.values()) {
            age.execute(graph, "MATCH (n:" + label.label() + ") WHERE n.district IS NOT NULL DETACH DELETE n", null);
        }
        jdbc.update("DELETE FROM public.crew_assignments WHERE crew_id LIKE 'crew-d%'");
        jdbc.update("DELETE FROM public.repair_crews WHERE crew_id LIKE 'crew-d%'");
    }

    private String rows(List<Map<String, Object>> chunk) {
        return objectMapper.writeValueAsString(Map.of("rows", chunk));
    }

    private static <T> List<List<T>> chunks(List<T> list) {
        List<List<T>> out = new ArrayList<>();
        for (int i = 0; i < list.size(); i += BATCH) {
            out.add(list.subList(i, Math.min(list.size(), i + BATCH)));
        }
        return out;
    }
}
