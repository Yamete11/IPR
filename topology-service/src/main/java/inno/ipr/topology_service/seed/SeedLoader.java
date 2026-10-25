package inno.ipr.topology_service.seed;

import inno.ipr.topology_service.age.AgeClient;
import inno.ipr.topology_service.domain.edge.EdgeType;
import inno.ipr.topology_service.domain.vertex.VertexLabel;
import inno.ipr.topology_service.seed.model.SeedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Loads {@code seed/city-topology.json} into the AGE graph on startup: creates the graph if
 * needed, then upserts every vertex and edge via {@code MERGE} (keyed by {@code id}), so
 * re-running the app never duplicates data.
 *
 * <p>Properties are applied with a {@code SET n.key = $p_key} clause using <em>scalar</em>
 * parameters. AGE 1.6.0 rejects a map-valued parameter in {@code SET n += $map}
 * ("SET clause expects a map"), so each property is bound individually.
 *
 * <p>Gated by {@code topology.seed.enabled} (default true) so it can be switched off — e.g. in
 * tests, where no AGE instance is available.
 *
 * <p>The graph written to is {@code topology.graph} — the same property the read side
 * ({@code TopologyRepository}) queries. The {@code graph} field in the seed file must agree with
 * it; a mismatch is rejected at startup rather than silently seeding a graph nobody reads.
 */
@Component
@ConditionalOnProperty(name = "topology.seed.enabled", havingValue = "true", matchIfMissing = true)
public class SeedLoader implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(SeedLoader.class);

    /** Property names are interpolated into Cypher as identifiers, so they must be safe ones. */
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

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
    public void run(ApplicationArguments args) throws Exception {
        SeedGraph seed;
        try (InputStream in = resourceLoader.getResource(location).getInputStream()) {
            seed = objectMapper.readValue(in, SeedGraph.class);
        }

        // The read side queries `topology.graph`; seeding a differently-named graph would succeed
        // here and then make every query return an empty list, with no error anywhere.
        String graph = seed.graph();
        if (graph == null || !graph.equals(configuredGraph)) {
            throw new IllegalStateException(
                    "Seed file '" + location + "' declares graph '" + graph + "' but topology.graph is '"
                            + configuredGraph + "'. They must match, or reads would find nothing.");
        }
        age.ensureGraph(graph);

        for (SeedGraph.Vertex v : seed.vertices()) {
            String label = VertexLabel.fromLabel(v.label()).label();
            Map<String, Object> params = new LinkedHashMap<>();
            params.put("id", v.id());
            StringBuilder cypher = new StringBuilder("MERGE (n:" + label + " {id: $id})");
            appendSet(cypher, "n", nullSafe(v.properties()), params);
            age.execute(graph, cypher.toString(), objectMapper.writeValueAsString(params));
        }

        // Edges after all vertices exist so the MATCH endpoints resolve.
        for (SeedGraph.Edge e : seed.edges()) {
            String type = EdgeType.fromType(e.type()).type();
            Map<String, Object> params = new LinkedHashMap<>();
            params.put("from", e.from());
            params.put("to", e.to());
            StringBuilder cypher = new StringBuilder(
                    "MATCH (a {id: $from}), (b {id: $to}) MERGE (a)-[r:" + type + "]->(b)");
            appendSet(cypher, "r", nullSafe(e.properties()), params);
            // RETURN so the row count distinguishes "edge written" from "MATCH found no endpoint".
            // Without it a typo'd from/to id is a silent no-op that quietly shrinks every
            // dependents/dependencies answer for the life of the graph.
            cypher.append(" RETURN r");
            int rows = age.execute(graph, cypher.toString(), objectMapper.writeValueAsString(params));
            if (rows == 0) {
                throw new IllegalStateException(
                        "Seed edge " + e.from() + " -[" + type + "]-> " + e.to()
                                + " was not created: no vertex matches one or both endpoint ids.");
            }
        }

        long vertexCount = age.count(graph, "MATCH (n) RETURN count(n)");
        long edgeCount = age.count(graph, "MATCH ()-[r]->() RETURN count(r)");
        log.info("Seeded '{}': graph now has {} vertices, {} edges (from {} vertex / {} edge seed entries)",
                graph, vertexCount, edgeCount, seed.vertices().size(), seed.edges().size());

        // Indexes come after the data: AGE materialises a label's table on the first vertex write,
        // so there is nothing to index before the seed has run.
        if (indexEnabled) {
            for (VertexLabel label : VertexLabel.values()) {
                age.ensurePropertyIndex(graph, label.label());
            }
            for (EdgeType type : EdgeType.values()) {
                age.ensureEdgeIndexes(graph, type.type());
            }
            log.info("Indexes ensured on '{}': properties of {} vertex labels, start_id/end_id of {} edge types",
                    graph, VertexLabel.values().length, EdgeType.values().length);
        }
    }

    /** Appends {@code SET var.key = $p_key, ...} for each property and binds the scalar params. */
    private static void appendSet(StringBuilder cypher, String var,
                                  Map<String, Object> props, Map<String, Object> params) {
        if (props.isEmpty()) {
            return;
        }
        cypher.append(" SET ");
        boolean first = true;
        for (Map.Entry<String, Object> entry : props.entrySet()) {
            String key = entry.getKey();
            if (!IDENTIFIER.matcher(key).matches()) {
                throw new IllegalArgumentException("Illegal property name in seed: " + key);
            }
            String param = "p_" + key;
            if (!first) {
                cypher.append(", ");
            }
            cypher.append(var).append('.').append(key).append(" = $").append(param);
            params.put(param, entry.getValue());
            first = false;
        }
    }

    private static Map<String, Object> nullSafe(Map<String, Object> props) {
        return props == null ? Map.of() : props;
    }
}
