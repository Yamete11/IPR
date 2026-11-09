package inno.ipr.incident_api_service.graphql.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * A repair crew on an incident, as ai-incident-responder-service resolved it from the AGE graph and
 * the crew tables and stored it in the {@code crews} jsonb column (snake_case keys, mapped here).
 * Component names match the {@code Crew} type in {@code schema.graphqls}.
 *
 * @param crewId crew id, e.g. {@code crew-grid}
 * @param name   display name
 * @param phone  dispatch phone; may be null
 * @param assets the incident's affected assets this crew maintains
 */
public record CrewDto(
        @JsonProperty("crew_id") String crewId,
        @JsonProperty("name") String name,
        @JsonProperty("phone") String phone,
        @JsonProperty("assets") List<String> assets) {
}
