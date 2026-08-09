package inno.ipr.ingestion_service.repository;

import inno.ipr.ingestion_service.telemetry.TelemetryMessage;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;

/**
 * Write side of the {@code metrics} hypertable — the only table ingestion-service owns. Plain JDBC
 * (not JPA), an <em>instance</em> {@link JdbcTemplate} injected via the constructor, mirroring
 * {@code topology-service}'s repository shape.
 *
 * <p>{@code ON CONFLICT DO NOTHING} against the {@code (timestamp, sensor_id, metric_type)} unique
 * index makes writes idempotent: a redelivered Kafka message is silently dropped instead of
 * duplicated — the Idempotent Consumer pattern without a separate dedup store.
 */
@Repository
public class MetricRepository {

    private static final String INSERT = """
            INSERT INTO metrics (timestamp, sensor_id, metric_type, value, status)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
            """;

    private final JdbcTemplate jdbc;

    public MetricRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * @return 1 if the reading was stored, 0 if an identical
     *         {@code (timestamp, sensor_id, metric_type)} row already existed and the insert was
     *         suppressed. Callers must not ignore the 0: it is the only signal that a measurement
     *         silently vanished, whether from a legitimate Kafka redelivery or from a producer
     *         emitting colliding timestamps.
     */
    public int save(TelemetryMessage m) {
        return jdbc.update(INSERT,
                Timestamp.from(m.timestamp()),
                m.sensorId(),
                m.metricType(),
                m.value(),
                m.status());
    }
}
