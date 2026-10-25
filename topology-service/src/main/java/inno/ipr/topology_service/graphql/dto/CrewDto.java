package inno.ipr.topology_service.graphql.dto;

import java.util.List;

/** A repair crew (relational table) and the affected assets (from the graph) it is assigned to. */
public record CrewDto(String crewId, String name, String phone, List<String> assets) {
}
