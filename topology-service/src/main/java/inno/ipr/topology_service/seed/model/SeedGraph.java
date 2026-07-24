package inno.ipr.topology_service.seed.model;

import java.util.List;
import java.util.Map;

/**
 * In-memory shape of {@code seed/city-topology.json}. Deliberately generic (label/type +
 * free-form properties) — the seed file describes the graph structurally, so richer per-label
 * domain records are not needed here; those belong to the read/GraphQL layer.
 *
 * <p>No {@code @JsonIgnoreProperties} is needed: Jackson 3 (and Spring Boot's mapper) ignore
 * unknown JSON properties by default, so extra fields in the seed file are tolerated.
 */
public record SeedGraph(
        String graph,
        List<Vertex> vertices,
        List<Edge> edges
) {

    public record Vertex(String label, String id, Map<String, Object> properties) {
    }

    public record Edge(String type, String from, String to, Map<String, Object> properties) {
    }
}
