package inno.ipr.topology_service.graphql.dto;

import java.util.List;

/** One directed path through the graph: {@code length} hops over {@code nodes} (start and end included). */
public record PathDto(int length, List<VertexDto> nodes) {
}
