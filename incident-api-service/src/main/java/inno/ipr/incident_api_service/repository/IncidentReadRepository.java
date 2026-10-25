package inno.ipr.incident_api_service.repository;

import inno.ipr.incident_api_service.graphql.dto.IncidentDto;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.ObjectMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * Read side of the {@code incidents} table (TimescaleDB); ai-incident-responder-service is its sole
 * writer.
 *
 * <p>{@code mapRow} parses the {@code impacted_objects} / {@code remediation_steps} {@code jsonb}
 * string arrays into lists with {@code parseStringArray}, so GraphQL exposes them as
 * {@code [String!]!}; anything other than a string array fails loudly.
 */
@Repository
public class IncidentReadRepository {

    private static final String COLUMNS = """
            incident_id, alert_id, sensor_id, metric_type, criticality,
            impacted_objects, remediation_steps, summary, model, created_at
            """;

    private static final String FIND_RECENT =
            "SELECT " + COLUMNS + " FROM incidents ORDER BY created_at DESC LIMIT ?";

    private static final String FIND_BY_ID =
            "SELECT " + COLUMNS + " FROM incidents WHERE incident_id = ?";

    private static final String FIND_BY_SENSOR =
            "SELECT " + COLUMNS + " FROM incidents WHERE sensor_id = ? ORDER BY created_at DESC LIMIT ?";

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public IncidentReadRepository(JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    public List<IncidentDto> findRecent(int limit) {
        return jdbc.query(FIND_RECENT, this::mapRow, limit);
    }

    public IncidentDto findById(String incidentId) {
        return jdbc.query(FIND_BY_ID, this::mapRow, incidentId).stream().findFirst().orElse(null);
    }

    public List<IncidentDto> findBySensor(String sensorId, int limit) {
        return jdbc.query(FIND_BY_SENSOR, this::mapRow, sensorId, limit);
    }

    private IncidentDto mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new IncidentDto(
                rs.getString("incident_id"),
                rs.getString("alert_id"),
                rs.getString("sensor_id"),
                rs.getString("metric_type"),
                rs.getString("criticality"),
                parseStringArray(rs.getString("impacted_objects")),
                parseStringArray(rs.getString("remediation_steps")),
                rs.getString("summary"),
                rs.getString("model"),
                rs.getTimestamp("created_at").toInstant().toString());
    }

    private List<String> parseStringArray(String json) {
        try {
            return Arrays.asList(objectMapper.readValue(json, String[].class));
        } catch (RuntimeException e) {
            throw new IllegalStateException("Failed to parse jsonb string array: " + json, e);
        }
    }
}
