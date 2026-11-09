package inno.ipr.incident_api_service.repository;

import inno.ipr.incident_api_service.graphql.dto.CrewDto;
import inno.ipr.incident_api_service.graphql.dto.IncidentDto;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.ObjectMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

/**
 * Read side of the {@code incidents} table (TimescaleDB); ai-incident-responder-service is its sole
 * writer and owns its schema, including the root-cause columns ({@code asset_id},
 * {@code root_incident_id}, {@code detected_at}, {@code crews}) it adds with {@code ALTER TABLE}.
 *
 * <p>{@link #findByRoot(String, int)} lists the incidents filed under a root, in detection order:
 * the secondary effects the responder correlated over the AGE graph.
 *
 * <p>{@code mapRow} parses the {@code impacted_objects} / {@code remediation_steps} {@code jsonb}
 * string arrays with {@code parseStringArray}, so GraphQL exposes them as {@code [String!]!}; anything
 * other than a string array fails loudly. {@code parseCrews} reads the {@code crews} array of objects;
 * a NULL column reads as no crews.
 */
@Repository
public class IncidentReadRepository {

    private static final String COLUMNS = """
            incident_id, alert_id, sensor_id, metric_type, asset_id, root_incident_id, detected_at,
            criticality, impacted_objects, remediation_steps, crews, summary, model, created_at
            """;

    private static final String FIND_RECENT =
            "SELECT " + COLUMNS + " FROM incidents ORDER BY created_at DESC LIMIT ?";

    private static final String FIND_BY_ID =
            "SELECT " + COLUMNS + " FROM incidents WHERE incident_id = ?";

    private static final String FIND_BY_SENSOR =
            "SELECT " + COLUMNS + " FROM incidents WHERE sensor_id = ? ORDER BY created_at DESC LIMIT ?";

    private static final String FIND_BY_ROOT =
            "SELECT " + COLUMNS + " FROM incidents WHERE root_incident_id = ? "
                    + "ORDER BY detected_at, created_at LIMIT ?";

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

    public List<IncidentDto> findByRoot(String rootIncidentId, int limit) {
        return jdbc.query(FIND_BY_ROOT, this::mapRow, rootIncidentId, limit);
    }

    private IncidentDto mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new IncidentDto(
                rs.getString("incident_id"),
                rs.getString("alert_id"),
                rs.getString("sensor_id"),
                rs.getString("metric_type"),
                rs.getString("asset_id"),
                rs.getString("root_incident_id"),
                iso(rs.getTimestamp("detected_at")),
                rs.getString("criticality"),
                parseStringArray(rs.getString("impacted_objects")),
                parseStringArray(rs.getString("remediation_steps")),
                parseCrews(rs.getString("crews")),
                rs.getString("summary"),
                rs.getString("model"),
                iso(rs.getTimestamp("created_at")));
    }

    private List<String> parseStringArray(String json) {
        try {
            return Arrays.asList(objectMapper.readValue(json, String[].class));
        } catch (RuntimeException e) {
            throw new IllegalStateException("Failed to parse jsonb string array: " + json, e);
        }
    }

    private List<CrewDto> parseCrews(String json) {
        if (json == null) {
            return List.of();
        }
        try {
            return Arrays.asList(objectMapper.readValue(json, CrewDto[].class));
        } catch (RuntimeException e) {
            throw new IllegalStateException("Failed to parse jsonb crews: " + json, e);
        }
    }

    private static String iso(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant().toString();
    }
}
