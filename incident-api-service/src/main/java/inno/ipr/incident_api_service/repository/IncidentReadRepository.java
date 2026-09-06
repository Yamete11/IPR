package inno.ipr.incident_api_service.repository;

import inno.ipr.incident_api_service.graphql.dto.IncidentDto;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.ObjectMapper;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

/**
 * Read side of the {@code incidents} table (TimescaleDB) — this service reads it but never writes
 * (ai-incident-responder-service is the sole writer). Plain JDBC, an instance {@link JdbcTemplate}.
 *
 * <p>The {@code impacted_objects} / {@code remediation_steps} columns are stored as {@code jsonb}
 * arrays of strings; the row mapper parses their text into {@code List<String>} with the shared
 * Jackson {@link ObjectMapper} so the GraphQL layer can expose them as {@code [String!]!}.
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
    private final RowMapper<IncidentDto> rowMapper;

    public IncidentReadRepository(JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.rowMapper = (rs, rowNum) -> {
            Timestamp createdAt = rs.getTimestamp("created_at");
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
                    createdAt == null ? null : createdAt.toInstant().toString());
        };
    }

    public List<IncidentDto> findRecent(int limit) {
        return jdbc.query(FIND_RECENT, rowMapper, limit);
    }

    public IncidentDto findById(String incidentId) {
        return jdbc.query(FIND_BY_ID, rowMapper, incidentId).stream().findFirst().orElse(null);
    }

    public List<IncidentDto> findBySensor(String sensorId, int limit) {
        return jdbc.query(FIND_BY_SENSOR, rowMapper, sensorId, limit);
    }

    /** Parses a {@code jsonb} array of strings (e.g. {@code ["node-5","bld-12"]}) to a list. */
    private List<String> parseStringArray(String json) {
        if (json == null || json.isBlank()) {
            return List.of();
        }
        try {
            return Arrays.asList(objectMapper.readValue(json, String[].class));
        } catch (RuntimeException e) {
            throw new IllegalStateException("Failed to parse jsonb string array: " + json, e);
        }
    }
}
