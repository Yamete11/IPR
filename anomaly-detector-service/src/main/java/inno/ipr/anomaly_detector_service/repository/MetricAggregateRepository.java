package inno.ipr.anomaly_detector_service.repository;

import inno.ipr.anomaly_detector_service.domain.MetricWindow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.util.List;

/**
 * Read side over the {@code metrics_5m_avg} continuous aggregate, plain JDBC. The view is created
 * with real-time aggregation on, so a just-arrived fault reading shows up here before the refresh
 * policy materialises its bucket.
 *
 * <p>{@link #findRecentWindows(Duration)} returns the <em>two</em> newest buckets for each series
 * seen within {@code lookback} of now, newest first. The newest alone is not enough: a fault in the
 * last seconds of a bucket is invisible to a poll that runs after the next bucket has started.
 * Buckets after {@code now()} are excluded so a future-dated reading cannot become the series'
 * "newest" window and hide its real data.
 */
@Repository
public class MetricAggregateRepository {

    private static final String FIND_RECENT = """
            SELECT sensor_id, metric_type, bucket, min_value, max_value
            FROM (
                SELECT *, row_number() OVER (PARTITION BY sensor_id, metric_type ORDER BY bucket DESC) AS rn
                FROM metrics_5m_avg
                WHERE bucket >= now() - ?::interval
                  AND bucket <= now()
            ) recent
            WHERE rn <= 2
            ORDER BY sensor_id, metric_type, bucket DESC
            """;

    private static final RowMapper<MetricWindow> ROW_MAPPER = (rs, rowNum) -> new MetricWindow(
            rs.getString("sensor_id"),
            rs.getString("metric_type"),
            rs.getTimestamp("bucket").toInstant(),
            rs.getDouble("min_value"),
            rs.getDouble("max_value"));

    private final JdbcTemplate jdbc;

    public MetricAggregateRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<MetricWindow> findRecentWindows(Duration lookback) {
        return jdbc.query(FIND_RECENT, ROW_MAPPER, lookback.toSeconds() + " seconds");
    }
}
