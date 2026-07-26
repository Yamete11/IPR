package inno.ipr.incident_api_service.repository;

import inno.ipr.incident_api_service.graphql.dto.MetricDto;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.List;

/**
 * Read side of the {@code metrics} hypertable (TimescaleDB) — recent raw readings for a sensor, for
 * the dashboard's charts. Read-only (ingestion-service is the sole writer). Instance
 * {@link JdbcTemplate}, no writes.
 */
@Repository
public class MetricReadRepository {

    private static final String FIND_RECENT_BY_SENSOR = """
            SELECT "timestamp", sensor_id, metric_type, value, status
            FROM metrics
            WHERE sensor_id = ?
            ORDER BY "timestamp" DESC
            LIMIT ?
            """;

    private static final RowMapper<MetricDto> ROW_MAPPER = (rs, rowNum) -> {
        Timestamp ts = rs.getTimestamp("timestamp");
        return new MetricDto(
                ts == null ? null : ts.toInstant().toString(),
                rs.getString("sensor_id"),
                rs.getString("metric_type"),
                rs.getDouble("value"),
                rs.getString("status"));
    };

    private final JdbcTemplate jdbc;

    public MetricReadRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<MetricDto> findRecentBySensor(String sensorId, int limit) {
        return jdbc.query(FIND_RECENT_BY_SENSOR, ROW_MAPPER, sensorId, limit);
    }
}
