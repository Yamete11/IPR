package inno.ipr.ai_incident_responder_service.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * A repair crew responsible for part of an incident's blast radius, resolved by the hybrid
 * SQL + Cypher query in {@code GraphRepository}: graph traversal for the affected assets, relational
 * {@code public.crew_assignments} / {@code public.repair_crews} for who maintains them. Stored on the
 * incident as JSON (snake_case, read back by incident-api) and listed in the Claude prompt.
 *
 * @param crewId crew id, e.g. {@code crew-grid}
 * @param name   display name, e.g. {@code Distribution grid crew}
 * @param phone  dispatch phone number; may be null
 * @param assets the affected assets this crew is assigned to, sorted
 */
public record Crew(
        @JsonProperty("crew_id") String crewId,
        @JsonProperty("name") String name,
        @JsonProperty("phone") String phone,
        @JsonProperty("assets") List<String> assets) {
}
