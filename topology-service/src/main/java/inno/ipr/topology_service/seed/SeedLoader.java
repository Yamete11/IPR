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

    public SeedLoader(AgeClient age,
                      ObjectMapper objectMapper,
                      ResourceLoader resourceLoader,
                      @Value("${topology.seed.location:classpath:seed/city-topology.json}") String location) {
        this.age = age;
        this.objectMapper = objectMapper;
        this.resourceLoader = resourceLoader;
        this.location = location;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        SeedGraph seed;
        try (InputStream in = resourceLoader.getResource(location).getInputStream()) {
            seed = objectMapper.readValue(in, SeedGraph.class);
        }

        String graph = seed.graph();
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
            age.execute(graph, cypher.toString(), objectMapper.writeValueAsString(params));
        }

        long vertexCount = age.count(graph, "MATCH (n) RETURN count(n)");
        long edgeCount = age.count(graph, "MATCH ()-[r]->() RETURN count(r)");
        log.info("Seeded '{}': graph now has {} vertices, {} edges (from {} vertex / {} edge seed entries)",
                graph, vertexCount, edgeCount, seed.vertices().size(), seed.edges().size());
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
