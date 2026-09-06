package inno.ipr.ingestion_service.schema;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies {@code schema.sql} against a real TimescaleDB — the hypertable and the advisory lock that
 * serialises concurrent replicas both depend on server behaviour no in-memory database reproduces.
 *
 * <p>Self-skips when Docker is unavailable so {@code ./gradlew build} still passes without it.
 */
@Testcontainers
@EnabledIf("dockerAvailable")
class SchemaInitializationIntegrationTest {

    /** Matches the image pinned in the repo's docker-compose.yml. */
    private static final DockerImageName IMAGE = DockerImageName
            .parse("timescale/timescaledb:2.28.3-pg16")
            .asCompatibleSubstituteFor("postgres");

    /** Must stay in step with the key in {@code schema.sql}. */
    private static final long LOCK_KEY = 91024001L;

    @Container
    @SuppressWarnings("resource")
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(IMAGE)
            .withDatabaseName("smartcity")
            .withUsername("smartcity")
            .withPassword("smartcity");

    static boolean dockerAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    private HikariDataSource dataSource;
    private JdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        dataSource = dataSource(0);
        jdbc = new JdbcTemplate(dataSource);
        // Each test starts from a clean slate: the populator's job is to build this from nothing.
        jdbc.execute("DROP TABLE IF EXISTS metrics CASCADE");
    }

    @AfterEach
    void tearDown() {
        dataSource.close();
    }

    /**
     * @param statementTimeoutMillis 0 to wait indefinitely. A non-zero value turns "blocked on the
     *                               advisory lock" into a prompt, deterministic error instead of a
     *                               hang — Postgres applies {@code statement_timeout} to a waiting
     *                               {@code pg_advisory_lock()} call.
     */
    private static HikariDataSource dataSource(int statementTimeoutMillis) {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(POSTGRES.getJdbcUrl());
        ds.setUsername(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        ds.setMaximumPoolSize(4);
        if (statementTimeoutMillis > 0) {
            ds.addDataSourceProperty("options", "-c statement_timeout=" + statementTimeoutMillis);
            ds.setConnectionInitSql("SET statement_timeout = " + statementTimeoutMillis);
        }
        return ds;
    }

    private static void runSchemaScript(DataSource target) {
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.addScript(new ClassPathResource("schema.sql"));
        populator.execute(target);
    }

    @Test
    @DisplayName("builds the hypertable and its idempotency index from nothing")
    void createsTheExpectedSchema() {
        runSchemaScript(dataSource);

        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = 'metrics'", Integer.class))
                .isOne();
        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'metrics'",
                Integer.class))
                .as("metrics must be time-partitioned, not a plain table")
                .isOne();
        assertThat(jdbc.queryForObject(
                "SELECT indexdef FROM pg_indexes WHERE indexname = 'metrics_uq'", String.class))
                .contains("UNIQUE")
                .contains("timestamp", "sensor_id", "metric_type");
    }

    @Test
    @DisplayName("partitions into 1-day chunks and indexes the series read path")
    void tunesChunkingAndIndexes() {
        runSchemaScript(dataSource);

        assertThat(jdbc.queryForObject("""
                SELECT time_interval::text FROM timescaledb_information.dimensions
                WHERE hypertable_name = 'metrics' AND column_name = 'timestamp'
                """, String.class))
                .as("chunk interval must be the 1 day the script asks for, not TimescaleDB's 7-day default")
                .isEqualTo("1 day");
        assertThat(jdbc.queryForObject(
                "SELECT indexdef FROM pg_indexes WHERE indexname = 'metrics_sensor_metric_ts'", String.class))
                .contains("sensor_id", "metric_type")
                .contains("\"timestamp\" DESC");
    }

    @Test
    @DisplayName("enables columnar compression and schedules the compression + retention policies")
    void configuresCompressionAndRetention() {
        runSchemaScript(dataSource);

        assertThat(jdbc.queryForObject(
                "SELECT compression_enabled FROM timescaledb_information.hypertables WHERE hypertable_name = 'metrics'",
                Boolean.class))
                .as("compression must be enabled before a compression policy can be added")
                .isTrue();
        assertThat(jdbc.queryForObject("""
                SELECT segmentby FROM timescaledb_information.hypertable_compression_settings
                WHERE hypertable = 'metrics'::regclass
                """, String.class))
                .contains("sensor_id").contains("metric_type");
        assertThat(policyJobs())
                .as("one compression job and one retention job on the hypertable")
                .containsExactlyInAnyOrder("policy_compression", "policy_retention");
        assertThat(jdbc.queryForObject("""
                SELECT config->>'compress_after' FROM timescaledb_information.jobs
                WHERE hypertable_name = 'metrics' AND proc_name = 'policy_compression'
                """, String.class)).isEqualTo("7 days");
        assertThat(jdbc.queryForObject("""
                SELECT config->>'drop_after' FROM timescaledb_information.jobs
                WHERE hypertable_name = 'metrics' AND proc_name = 'policy_retention'
                """, String.class)).isEqualTo("30 days");
    }

    @Test
    @DisplayName("re-running on an existing schema is a no-op, not an error")
    void isIdempotentAcrossRestarts() {
        runSchemaScript(dataSource);

        assertThatCode(() -> runSchemaScript(dataSource)).doesNotThrowAnyException();

        assertThat(jdbc.queryForObject("SELECT count(*) FROM pg_indexes WHERE indexname = 'metrics_uq'",
                Integer.class)).isOne();
        assertThat(policyJobs())
                .as("if_not_exists must stop the policies from being registered twice")
                .containsExactlyInAnyOrder("policy_compression", "policy_retention");
    }

    private List<String> policyJobs() {
        return jdbc.queryForList("""
                SELECT proc_name FROM timescaledb_information.jobs
                WHERE hypertable_name = 'metrics' AND proc_name IN ('policy_compression', 'policy_retention')
                """, String.class);
    }

    /**
     * The deterministic half of the concurrency fix. Holding the lock from another session must
     * block the script; without the {@code pg_advisory_lock} call it sails straight through and the
     * DDL races instead.
     */
    @Test
    @DisplayName("the script waits on the advisory lock another replica already holds")
    void blocksWhileAnotherReplicaHoldsTheLock() throws Exception {
        try (HikariDataSource holder = dataSource(0);
             HikariDataSource blocked = dataSource(3_000)) {

            JdbcTemplate holderJdbc = new JdbcTemplate(holder);
            holderJdbc.execute("SET idle_in_transaction_session_timeout = 0");
            holderJdbc.queryForObject("SELECT pg_advisory_lock(?)", String.class, LOCK_KEY);

            assertThatThrownBy(() -> runSchemaScript(blocked))
                    .as("a second replica must wait rather than race the DDL")
                    // The statement it died on names the lock, so this cannot pass by some unrelated
                    // timeout: without the pg_advisory_lock call the script would run straight to
                    // completion here.
                    .hasMessageContaining("pg_advisory_lock(" + LOCK_KEY + ")")
                    .rootCause()
                    .hasMessageContaining("canceling statement due to statement timeout");

            holderJdbc.queryForObject("SELECT pg_advisory_unlock(?)", Boolean.class, LOCK_KEY);

            assertThatCode(() -> runSchemaScript(blocked))
                    .as("and must proceed once the lock is free")
                    .doesNotThrowAnyException();
        }
    }

    @Test
    @DisplayName("two replicas starting together both succeed")
    void concurrentInitialisationBothSucceed() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try (HikariDataSource a = dataSource(0); HikariDataSource b = dataSource(0)) {
            List<Future<Boolean>> results = pool.invokeAll(List.<Callable<Boolean>>of(
                    () -> { runSchemaScript(a); return true; },
                    () -> { runSchemaScript(b); return true; }));

            // get() rethrows whatever the thread hit, so a DDL race surfaces here as a failure.
            for (Future<Boolean> result : results) {
                assertThat(result.get()).isTrue();
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'metrics'",
                Integer.class)).isOne();
    }

    /**
     * A session-level lock outlives the script if the unlock is ever dropped, and a pooled
     * connection would then carry it back into the pool and wedge every later start-up.
     */
    @Test
    @DisplayName("the advisory lock is released when the script finishes")
    void releasesTheLockAfterwards() {
        runSchemaScript(dataSource);

        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'", Integer.class))
                .as("no advisory lock may outlive the script")
                .isZero();
    }
}
