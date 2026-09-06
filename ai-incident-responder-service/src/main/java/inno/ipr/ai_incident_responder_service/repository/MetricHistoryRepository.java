package inno.ipr.ai_incident_responder_service.repository;

import inno.ipr.ai_incident_responder_service.domain.MetricSummary;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;

/**
 * Read side over the {@code metrics} hypertable (written by ingestion-service) — the last hour of a
 * sensor's readings, aggregated into a {@link MetricSummary}. Uses the <em>TimescaleDB</em>
 * {@code JdbcTemplate}. Aggregating in SQL keeps the payload tiny regardless of the ingestion rate.
 */
@Repository
public class MetricHistoryRepository {

    /**
     * One summary row for the sensor's last hour of <em>this metric</em>.
     * {@code array_agg(... ORDER BY timestamp DESC)[1]} is the most recent value;
     * {@code max(timestamp)} its instant. All aggregates are {@code NULL} when there are no readings
     * in the window (count 0).
     *
     * <p>The {@code metric_type} filter is not optional. The hypertable is keyed on
     * {@code (timestamp, sensor_id, metric_type)} and the telemetry contract lets one sensor report
     * several metrics; without the filter a min/max/avg would blend, say, pipe pressure with cable
     * temperature and hand Claude a summary that describes no real quantity at all.
     */
    private static final String SUMMARY = """
            SELECT count(*)                                            AS cnt,
                   min(value)                                          AS mn,
                   max(value)                                          AS mx,
                   avg(value)                                          AS av,
                   (array_agg(value ORDER BY "timestamp" DESC))[1]     AS latest,
                   max("timestamp")                                    AS latest_at
            FROM metrics
            WHERE sensor_id = ?
              AND metric_type = ?
              AND "timestamp" >= now() - INTERVAL '1 hour'
            """;

    private final JdbcTemplate jdbc;

    public MetricHistoryRepository(@Qualifier("timescaleJdbc") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public MetricSummary findLastHour(String sensorId, String metricType) {
        return jdbc.queryForObject(SUMMARY, (rs, rowNum) -> {
            Timestamp latestAt = rs.getTimestamp("latest_at");
            return new MetricSummary(
                    rs.getLong("cnt"),
                    rs.getObject("mn", Double.class),
                    rs.getObject("mx", Double.class),
                    rs.getObject("av", Double.class),
                    rs.getObject("latest", Double.class),
                    latestAt == null ? null : latestAt.toInstant());
        }, sensorId, metricType);
    }
}
