package inno.ipr.ingestion_service.repository;

import inno.ipr.ingestion_service.telemetry.TelemetryMessage;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

/**
 * Write side of the {@code metrics} hypertable — the only table ingestion-service owns. Plain JDBC
 * (not JPA), an <em>instance</em> {@link JdbcTemplate} injected via the constructor, mirroring
 * {@code topology-service}'s repository shape.
 *
 * <p>{@code ON CONFLICT DO NOTHING} against the {@code (timestamp, sensor_id, metric_type)} unique
 * index makes writes idempotent: a redelivered Kafka message is silently dropped instead of
 * duplicated — the Idempotent Consumer pattern without a separate dedup store.
 *
 * <p>{@link #saveAll} is the hot path: one JDBC batch per Kafka poll. With
 * {@code reWriteBatchedInserts=true} on the JDBC URL the driver collapses the batch into a single
 * multi-row {@code INSERT ... VALUES (...), (...)} round trip, which is where the throughput comes
 * from. {@link #save} remains for the per-row fallback the consumer uses to pin down which row of
 * a failed batch the database rejected.
 *
 * <p>{@link #save(TelemetryMessage)} returns 1 if the reading was stored, 0 if an identical
 * {@code (timestamp, sensor_id, metric_type)} row already existed and the insert was suppressed.
 * Callers must not ignore the 0: it is the only signal that a measurement silently vanished,
 * whether from a legitimate Kafka redelivery or from a producer emitting colliding timestamps.
 *
 * <p>{@link #saveAll(List)} returns one entry per message, in order: {@code 1} stored, {@code 0}
 * suppressed as a duplicate, or {@link java.sql.Statement#SUCCESS_NO_INFO} ({@code -2}) when the
 * driver rewrote the batch into a multi-row statement and cannot attribute row counts — the normal
 * case with {@code reWriteBatchedInserts=true}, so duplicate counting is best-effort there and
 * exact only through {@link #save}.
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

    public int save(TelemetryMessage m) {
        return jdbc.update(INSERT,
                Timestamp.from(m.timestamp()),
                m.sensorId(),
                m.metricType(),
                m.value(),
                m.status());
    }

    public int[] saveAll(List<TelemetryMessage> messages) {
        return jdbc.batchUpdate(INSERT, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                TelemetryMessage m = messages.get(i);
                ps.setTimestamp(1, Timestamp.from(m.timestamp()));
                ps.setString(2, m.sensorId());
                ps.setString(3, m.metricType());
                ps.setDouble(4, m.value());
                ps.setString(5, m.status());
            }

            @Override
            public int getBatchSize() {
                return messages.size();
            }
        });
    }
}
