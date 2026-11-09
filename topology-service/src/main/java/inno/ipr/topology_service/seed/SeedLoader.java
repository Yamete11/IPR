package inno.ipr.topology_service.seed;

import inno.ipr.topology_service.age.AgeClient;
import inno.ipr.topology_service.domain.EdgeType;
import inno.ipr.topology_service.domain.VertexLabel;
import inno.ipr.topology_service.seed.model.SeedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Loads {@code seed/city-topology.json} into the AGE graph on startup: creates the graph if
 * needed, then upserts every vertex and edge via {@code MERGE} (keyed by {@code id}), so
 * re-running the app never duplicates data.
 *
 * <p>Gated by {@code topology.seed.enabled} (default true) so it can be switched off — e.g. in
 * tests, where no AGE instance is available.
 *
 * <p>{@link #run(ApplicationArguments)} reads the seed, checks its graph name, writes vertices, then
 * edges (so their {@code MATCH} endpoints resolve), logs the resulting counts and finally ensures the
 * indexes — after the data, because AGE only materialises a label's table on its first write. The
 * graph written to is {@code topology.graph}, the same property {@code TopologyRepository} reads; a
 * seed file declaring another graph is rejected, since it would seed fine and then make every query
 * silently return an empty list.
 *
 * <p>{@code seedVertices(String, List)} merges each vertex under its allow-listed {@link VertexLabel}.
 *
 * <p>{@code seedEdges(String, List)} merges each edge under its allow-listed {@link EdgeType}. The
 * statement ends in {@code RETURN r} so the row count tells "written" from "{@code MATCH} found no
 * endpoint"; without it a typo'd from/to id would be a silent no-op that quietly shrinks every
 * dependents/dependencies answer.
 *
 * <p>{@code ensureIndexes(String)} creates the GIN property index on every vertex label and the
 * {@code start_id}/{@code end_id} indexes on every edge type, when {@code topology.seed.index-enabled}.
 *
 * <p>{@code appendSet(StringBuilder, String, Map, Map)} appends {@code SET var.key = $p_key, ...} and
 * binds each property as its own <em>scalar</em> parameter: AGE 1.6.0 rejects a map-valued parameter
 * in {@code SET n += $map} ("SET clause expects a map"). Property names are interpolated as
 * identifiers, so each is checked with {@link AgeClient#checkIdentifier(String, String)} first.
 *
 * <p>{@code @Order(1)}: runs before {@code SyntheticCityRunner}, which adds the generated city.
 */
@Component
@Order(1)
@ConditionalOnProperty(name = "topology.seed.enabled", havingValue = "true", matchIfMissing = true)
public class SeedLoader implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(SeedLoader.class);

    private final AgeClient age;
    private final ObjectMapper objectMapper;
    private final ResourceLoader resourceLoader;
    private final String location;
    private final String configuredGraph;
    private final boolean indexEnabled;

    public SeedLoader(AgeClient age,
                      ObjectMapper objectMapper,
                      ResourceLoader resourceLoader,
                      @Value("${topology.seed.location:classpath:seed/city-topology.json}") String location,
                      @Value("${topology.graph:city_topology}") String configuredGraph,
                      @Value("${topology.seed.index-enabled:true}") boolean indexEnabled) {
        this.age = age;
        this.objectMapper = objectMapper;
        this.resourceLoader = resourceLoader;
        this.location = location;
        this.configuredGraph = configuredGraph;
        this.indexEnabled = indexEnabled;
    }

    @Override
    public void run(ApplicationArguments args) throws IOException {
        SeedGraph seed = readSeed();
        String graph = seed.graph();
        if (graph == null || !graph.equals(configuredGraph)) {
            throw new IllegalStateException(
                    "Seed file '" + location + "' declares graph '" + graph + "' but topology.graph is '"
                            + configuredGraph + "'. They must match, or reads would find nothing.");
        }
        age.ensureGraph(graph);

        seedVertices(graph, seed.vertices());
        seedEdges(graph, seed.edges());
        log.info("Seeded '{}': graph now has {} vertices, {} edges (from {} vertex / {} edge seed entries)",
                graph,
                age.count(graph, "MATCH (n) RETURN count(n)"),
                age.count(graph, "MATCH ()-[r]->() RETURN count(r)"),
                seed.vertices().size(), seed.edges().size());

        if (indexEnabled) {
            ensureIndexes(graph);
        }
    }

    private SeedGraph readSeed() throws IOException {
        try (InputStream in = resourceLoader.getResource(location).getInputStream()) {
            return objectMapper.readValue(in, SeedGraph.class);
        }
    }

    private void seedVertices(String graph, List<SeedGraph.Vertex> vertices) {
        for (SeedGraph.Vertex v : vertices) {
            String label = VertexLabel.fromLabel(v.label()).label();
            Map<String, Object> params = new LinkedHashMap<>();
            params.put("id", v.id());
            StringBuilder cypher = new StringBuilder("MERGE (n:" + label + " {id: $id})");
            appendSet(cypher, "n", v.properties(), params);
            age.execute(graph, cypher.toString(), objectMapper.writeValueAsString(params));
        }
    }

    private void seedEdges(String graph, List<SeedGraph.Edge> edges) {
        for (SeedGraph.Edge e : edges) {
            String type = EdgeType.fromType(e.type()).type();
            Map<String, Object> params = new LinkedHashMap<>();
            params.put("from", e.from());
            params.put("to", e.to());
            StringBuilder cypher = new StringBuilder(
                    "MATCH (a {id: $from}), (b {id: $to}) MERGE (a)-[r:" + type + "]->(b)");
            appendSet(cypher, "r", e.properties(), params);
            cypher.append(" RETURN r");
            int rows = age.execute(graph, cypher.toString(), objectMapper.writeValueAsString(params));
            if (rows == 0) {
                throw new IllegalStateException(
                        "Seed edge " + e.from() + " -[" + type + "]-> " + e.to()
                                + " was not created: no vertex matches one or both endpoint ids.");
            }
        }
    }

    private void ensureIndexes(String graph) {
        for (VertexLabel label : VertexLabel.values()) {
            age.ensurePropertyIndex(graph, label.label());
        }
        for (EdgeType type : EdgeType.values()) {
            age.ensureEdgeIndexes(graph, type.type());
        }
        log.info("Indexes ensured on '{}': properties of {} vertex labels, start_id/end_id of {} edge types",
                graph, VertexLabel.values().length, EdgeType.values().length);
    }

    private static void appendSet(StringBuilder cypher, String var,
                                  Map<String, Object> props, Map<String, Object> params) {
        if (props == null || props.isEmpty()) {
            return;
        }
        String separator = " SET ";
        for (Map.Entry<String, Object> entry : props.entrySet()) {
            String key = entry.getKey();
            AgeClient.checkIdentifier("property", key);
            String param = "p_" + key;
            cypher.append(separator).append(var).append('.').append(key).append(" = $").append(param);
            params.put(param, entry.getValue());
            separator = ", ";
        }
    }
}
