package inno.ipr.topology_service.graphql.dto;

import java.util.Map;

public record EdgeDto(String type, String from, String to, Map<String, Object> properties) {
}
