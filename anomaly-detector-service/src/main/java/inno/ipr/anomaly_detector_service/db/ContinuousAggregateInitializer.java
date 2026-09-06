package inno.ipr.anomaly_detector_service.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Owns the deferred 5-minute continuous aggregate that ingestion-service left out. Creates
 * {@code metrics_5m_avg} on startup — a {@code time_bucket}'d rollup of the {@code metrics}
 * hypertable carrying avg/min/max/count per {@code (sensor_id, metric_type)} — plus a refresh
 * policy. Depends on the hypertable already existing (ingestion-service creates it).
 *
 * <p>That dependency is a start-order hazard in both compose and Kubernetes: nothing guarantees
 * ingestion-service has run its schema script before this runner fires, and
 * {@code CREATE MATERIALIZED VIEW ... FROM metrics} against a missing table fails the context and
 * crash-loops the pod. So the runner first {@linkplain #awaitHypertable() waits} for the hypertable
 * to appear, up to {@code anomaly.schema.wait-timeout}, polling every
 * {@code anomaly.schema.wait-poll}. Only a genuine misconfiguration (ingestion never starts) turns
 * into a failure, and then a loud one.
 *
 * <p>The DDL runs via {@link JdbcTemplate#execute(String)} rather than {@code spring.sql.init}
 * because {@code CREATE MATERIALIZED VIEW ... WITH (timescaledb.continuous)} cannot execute inside
 * a transaction block, and the script initializer may wrap statements in one. JdbcTemplate runs
 * each statement on an autocommit connection, which TimescaleDB accepts. Every statement is
 * idempotent ({@code IF NOT EXISTS} / {@code if_not_exists => TRUE}), so re-running the app is a
 * no-op. Mirrors topology-service's {@code SeedLoader}: an {@link ApplicationRunner} gated by a
 * property so tests (with no live DB) can switch it off.
 */
@Component
@ConditionalOnProperty(name = "anomaly.schema.init-enabled", havingValue = "true", matchIfMissing = true)
public class ContinuousAggregateInitializer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(ContinuousAggregateInitializer.class);

    /**
     * Detection reads min/max (a single fault reading averaged over hundreds of OK readings would
     * never cross an avg threshold), so the aggregate materialises the boundary values too.
     */
    private static final String CREATE_VIEW = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_5m_avg
            WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
            SELECT time_bucket('5 minutes', "timestamp") AS bucket,
                   sensor_id,
                   metric_type,
                   avg(value) AS avg_value,
                   min(value) AS min_value,
                   max(value) AS max_value,
                   count(*)   AS sample_count
            FROM metrics
            GROUP BY bucket, sensor_id, metric_type
            WITH NO DATA
            """;

    /**
     * Force real-time aggregation on. Modern TimescaleDB (2.13+) defaults
     * {@code materialized_only} to <em>true</em>, so a fresh continuous aggregate created
     * {@code WITH NO DATA} would return nothing until its refresh policy first runs. Setting it
     * false makes queries union the materialised buckets with the recent not-yet-materialised rows,
     * so a just-arrived fault reading is visible immediately. Idempotent, and also converges a view
     * created by an older build of this initializer.
     */
    private static final String ENABLE_REALTIME =
            "ALTER MATERIALIZED VIEW metrics_5m_avg SET (timescaledb.materialized_only = false)";

    private static final String ADD_POLICY = """
            SELECT add_continuous_aggregate_policy('metrics_5m_avg',
                start_offset      => INTERVAL '1 hour',
                end_offset        => INTERVAL '10 minutes',
                schedule_interval => INTERVAL '1 minute',
                if_not_exists     => TRUE)
            """;

    /** A hypertable, specifically — a plain {@code metrics} table would make the view creation fail. */
    private static final String HYPERTABLE_PRESENT =
            "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'metrics'";

    private final JdbcTemplate jdbc;
    private final Duration waitTimeout;
    private final Duration waitPoll;

    public ContinuousAggregateInitializer(JdbcTemplate jdbc,
                                          @Value("${anomaly.schema.wait-timeout:5m}") Duration waitTimeout,
                                          @Value("${anomaly.schema.wait-poll:5s}") Duration waitPoll) {
        this.jdbc = jdbc;
        this.waitTimeout = waitTimeout;
        this.waitPoll = waitPoll;
    }

    @Override
    public void run(ApplicationArguments args) throws InterruptedException {
        awaitHypertable();
        jdbc.execute(CREATE_VIEW);
        jdbc.execute(ENABLE_REALTIME);
        jdbc.execute(ADD_POLICY);
        log.info("Continuous aggregate 'metrics_5m_avg' ready (real-time aggregation on; refresh policy ensured)");
    }

    /**
     * Blocks until the {@code metrics} hypertable exists, treating an unreachable database the same
     * way as a missing table (both mean "ingestion has not finished starting yet").
     */
    private void awaitHypertable() throws InterruptedException {
        Instant deadline = Instant.now().plus(waitTimeout);
        while (true) {
            try {
                Integer count = jdbc.queryForObject(HYPERTABLE_PRESENT, Integer.class);
                if (count != null && count > 0) {
                    return;
                }
                log.info("Waiting for the 'metrics' hypertable (created by ingestion-service); retry in {}", waitPoll);
            } catch (DataAccessException e) {
                log.info("Waiting for TimescaleDB ({}); retry in {}", e.getMostSpecificCause().getMessage(), waitPoll);
            }
            if (Instant.now().isAfter(deadline)) {
                throw new IllegalStateException("The 'metrics' hypertable did not appear within " + waitTimeout
                        + "; is ingestion-service running against the same TimescaleDB?");
            }
            Thread.sleep(waitPoll.toMillis());
        }
    }
}
