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
 * Creates the {@code metrics_5m_avg} continuous aggregate (avg/min/max/count per series per 5-minute
 * bucket) and its refresh policy on startup, over the {@code metrics} hypertable that
 * ingestion-service owns. Gated by {@code anomaly.schema.init-enabled} so tests without a database
 * switch it off.
 *
 * <p>The DDL runs through {@link JdbcTemplate#execute(String)} on an autocommit connection rather
 * than {@code spring.sql.init}, because a continuous aggregate cannot be created inside a transaction
 * block. Every statement is idempotent, so a restart is a no-op.
 *
 * <p>{@code CREATE_VIEW} keeps min/max because detection reads them: one fault reading averaged over
 * hundreds of healthy ones would never cross an avg threshold.
 *
 * <p>{@code ENABLE_REALTIME} turns real-time aggregation on. TimescaleDB 2.13+ defaults
 * {@code materialized_only} to true, so a fresh {@code WITH NO DATA} view would return nothing until
 * its first refresh. Kept alongside the {@code CREATE} option so a view from an older build converges.
 *
 * <p>{@link #awaitHypertable()} blocks until {@code HYPERTABLE_PRESENT} finds the hypertable, polling
 * every {@code anomaly.schema.wait-poll} for up to {@code anomaly.schema.wait-timeout}. Nothing
 * orders this service after ingestion in compose or Kubernetes, and creating the view over a missing
 * table would crash-loop the pod. An unreachable database counts as "not there yet".
 */
@Component
@ConditionalOnProperty(name = "anomaly.schema.init-enabled", havingValue = "true", matchIfMissing = true)
public class ContinuousAggregateInitializer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(ContinuousAggregateInitializer.class);

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

    private static final String ENABLE_REALTIME =
            "ALTER MATERIALIZED VIEW metrics_5m_avg SET (timescaledb.materialized_only = false)";

    private static final String ADD_POLICY = """
            SELECT add_continuous_aggregate_policy('metrics_5m_avg',
                start_offset      => INTERVAL '1 hour',
                end_offset        => INTERVAL '10 minutes',
                schedule_interval => INTERVAL '1 minute',
                if_not_exists     => TRUE)
            """;

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
