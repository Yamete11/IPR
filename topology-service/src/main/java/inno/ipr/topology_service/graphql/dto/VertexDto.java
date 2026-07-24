package inno.ipr.topology_service.graphql.dto;

import java.util.Map;

public record VertexDto(String id, String label, Map<String, Object> properties) {
}
