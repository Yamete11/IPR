package inno.ipr.anomaly_detector_service.repository;

import inno.ipr.anomaly_detector_service.domain.MetricWindow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.util.List;

/**
 * Read side over the {@code metrics_5m_avg} continuous aggregate. Plain JDBC (not JPA), an
 * <em>instance</em> {@link JdbcTemplate} injected via the constructor — the same shape as
 * ingestion-service's {@code MetricRepository}.
 *
 * <p>Real-time aggregation (TimescaleDB's default for continuous aggregates) folds the most recent
 * not-yet-materialised rows into query results, so a just-arrived fault reading shows up here well
 * before the refresh policy runs — detection is near-immediate.
 */
@Repository
public class MetricAggregateRepository {

    /**
     * Latest bucket per series within the lookback window. {@code DISTINCT ON (sensor_id,
     * metric_type)} with the matching {@code ORDER BY ... bucket DESC} collapses each series to its
     * newest bucket. The lookback is bound as a Postgres {@code interval} literal.
     */
    private static final String FIND_LATEST = """
            SELECT DISTINCT ON (sensor_id, metric_type)
                   sensor_id, metric_type, bucket, avg_value, min_value, max_value, sample_count
            FROM metrics_5m_avg
            WHERE bucket >= now() - ?::interval
            ORDER BY sensor_id, metric_type, bucket DESC
            """;

    private static final RowMapper<MetricWindow> ROW_MAPPER = (rs, rowNum) -> new MetricWindow(
            rs.getString("sensor_id"),
            rs.getString("metric_type"),
            rs.getTimestamp("bucket").toInstant(),
            rs.getDouble("avg_value"),
            rs.getDouble("min_value"),
            rs.getDouble("max_value"),
            rs.getLong("sample_count"));

    private final JdbcTemplate jdbc;

    public MetricAggregateRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * The newest 5-minute window for each {@code (sensor_id, metric_type)} seen within
     * {@code lookback} of now.
     */
    public List<MetricWindow> findLatestWindows(Duration lookback) {
        String interval = lookback.toSeconds() + " seconds";
        return jdbc.query(FIND_LATEST, ROW_MAPPER, interval);
    }
}
