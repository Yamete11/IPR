package inno.ipr.ingestion_service.benchmark;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The storage side of the time-series design, measured on one synthetic data set loaded twice: into
 * a plain PostgreSQL table with B-tree indexes ({@code plain_metrics}) and into the real
 * {@code metrics} hypertable built by {@code schema.sql}. Walks the whole data lifecycle — load,
 * chunk exclusion, continuous aggregate vs. materialized view, space partitioning, columnar
 * compression, tiering to a second tablespace, retention — and asserts each mechanism did what the
 * docs say it does.
 *
 * <p>Data: {@value #SENSORS} sensors × 2 metrics × one reading a minute × {@value #DAYS} days, ending at
 * today's UTC midnight so every 1-day chunk is full. Run with {@code ./gradlew timeseriesBenchmark};
 * excluded from {@code build}. Writes {@code build/reports/timeseries-storage.md}.
 * {@link #generate(String, Instant, Instant)} produces one reading a minute for every sensor and
 * both metrics in {@code [from, to)}, a smooth signal plus noise. {@code end} is the exclusive end
 * of the loaded data, today's UTC midnight, so day {@code d} ({@link #dayStart(int)}) is one whole
 * chunk. {@code CHUNK} matches a chunk relation in a plan but not its {@code compress_hyper_...}
 * companion.
 *
 * <ul>
 *   <li>{@link #setUpTables()}: the production schema, unchanged; its background policies are
 *       removed so the run decides when chunks get compressed or dropped, not the job scheduler.
 *       {@code plain_metrics} has the same columns and the same two indexes, as an ordinary heap
 *       table.</li>
 *   <li>{@link #load()}: one day at a time into each table, so the per-day cost shows how inserts
 *       behave as the table grows.</li>
 *   <li>{@link #btreeVersusChunks()}: for the last hour across every sensor, the plain table walks
 *       its one big index while the hypertable first throws away every chunk whose time range
 *       cannot match.</li>
 *   <li>{@link #continuousAggregateVersusMaterializedView()}: the continuous aggregate is built
 *       exactly as anomaly-detector creates {@code metrics_5m_avg}. Five fresh minutes then arrive
 *       after both were built. Catching up, the CA materialises only the window it is asked for
 *       while the MV recomputes everything. Finally a late reading lands in an already-materialised
 *       bucket a day back: real-time aggregation only covers buckets past the watermark, so the CA
 *       serves the stale bucket until the invalidation log is processed — and that refresh
 *       recomputes just the invalidated bucket.</li>
 *   <li>{@link #compression()}: compresses everything older than 3 days, the "warm" tier.</li>
 *   <li>{@link #tiering()}: self-hosted tiering, where a second tablespace stands in for cheaper,
 *       slower storage.</li>
 * </ul>
 */
@Tag("benchmark")
@Testcontainers
@EnabledIf("dockerAvailable")
class TimeseriesStorageBenchmarkTest {

    private static final DockerImageName IMAGE = DockerImageName
            .parse("timescale/timescaledb:2.28.3-pg16")
            .asCompatibleSubstituteFor("postgres");

    private static final int SENSORS = 100;
    private static final int DAYS = 10;
    private static final int WARMUP = 2;
    private static final int RUNS = 7;
    private static final String TABLESPACE_DIR = "/var/lib/postgresql/history";

    @Container
    @SuppressWarnings("resource")
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(IMAGE)
            .withDatabaseName("smartcity")
            .withUsername("smartcity")
            .withPassword("smartcity");

    static boolean dockerAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    private static final Pattern CHUNK = Pattern.compile("\\b_hyper_\\d+_\\d+_chunk\\b");

    private static HikariDataSource dataSource;
    private static JdbcTemplate jdbc;
    private static Instant end;

    private final StringBuilder report = new StringBuilder();

    @BeforeAll
    static void connect() {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(POSTGRES.getJdbcUrl());
        dataSource.setUsername(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        dataSource.setMaximumPoolSize(2);
        jdbc = new JdbcTemplate(dataSource);
        end = Instant.now().truncatedTo(ChronoUnit.DAYS);
    }

    @AfterAll
    static void close() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @Test
    void timeseriesStorageLifecycle() throws Exception {
        setUpTables();
        load();
        btreeVersusChunks();
        continuousAggregateVersusMaterializedView();
        spacePartitioning();
        compression();
        tiering();
        retention();

        String text = header() + report;
        Path out = Path.of("build", "reports", "timeseries-storage.md");
        Files.createDirectories(out.getParent());
        Files.writeString(out, text);
        System.out.println(text);
    }

    private void setUpTables() {
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.addScript(new ClassPathResource("schema.sql"));
        populator.execute(dataSource);
        jdbc.execute("SELECT remove_compression_policy('metrics')");
        jdbc.execute("SELECT remove_retention_policy('metrics')");

        jdbc.execute("""
                CREATE TABLE plain_metrics (
                    timestamp   TIMESTAMPTZ      NOT NULL,
                    sensor_id   TEXT             NOT NULL,
                    metric_type TEXT             NOT NULL,
                    value       DOUBLE PRECISION NOT NULL,
                    status      TEXT             NOT NULL
                )""");
        jdbc.execute("CREATE UNIQUE INDEX plain_metrics_uq ON plain_metrics (timestamp, sensor_id, metric_type)");
        jdbc.execute("CREATE INDEX plain_metrics_sensor_metric_ts ON plain_metrics (sensor_id, metric_type, timestamp DESC)");
    }

    private void load() {
        List<double[]> perDay = new ArrayList<>();
        for (int d = 0; d < DAYS; d++) {
            Instant from = dayStart(d);
            Instant to = from.plus(Duration.ofDays(1));
            double plain = millis(() -> jdbc.update(generate("plain_metrics", from, to)));
            double hyper = millis(() -> jdbc.update(generate("metrics", from, to)));
            perDay.add(new double[]{plain, hyper});
        }
        jdbc.execute("ANALYZE plain_metrics");
        jdbc.execute("ANALYZE metrics");

        long rows = count("plain_metrics", "TRUE");
        assertThat(count("metrics", "TRUE")).isEqualTo(rows).isEqualTo((long) SENSORS * 2 * 1440 * DAYS);

        section("1. Load: one more day into a growing table");
        report.append(String.format(Locale.ROOT,
                "%,d rows per table (%,d rows/day). Each day is one `INSERT ... SELECT generate_series`, "
                        + "so the numbers are server-side insert + index maintenance only.%n%n", rows, rows / DAYS));
        report.append("| Day | Rows already in table | Plain table, ms | Hypertable, ms |\n|---:|---:|---:|---:|\n");
        for (int d = 0; d < perDay.size(); d++) {
            report.append(String.format(Locale.ROOT, "| %d | %,d | %s | %s |%n",
                    d + 1, rows / DAYS * d, fmt(perDay.get(d)[0]), fmt(perDay.get(d)[1])));
        }
    }

    private void btreeVersusChunks() {
        long plainHeap = queryLong("SELECT pg_relation_size('plain_metrics')");
        long plainIndexes = queryLong("SELECT pg_indexes_size('plain_metrics')");
        long hyperIndexes = queryLong("SELECT index_bytes FROM hypertable_detailed_size('metrics')");
        long chunks = queryLong("SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'metrics'");
        long newestChunkIndexes = queryLong("""
                SELECT s.index_bytes
                FROM chunks_detailed_size('metrics') s
                JOIN timescaledb_information.chunks c ON c.chunk_schema = s.chunk_schema AND c.chunk_name = s.chunk_name
                ORDER BY c.range_end DESC LIMIT 1""");
        assertThat(chunks).isEqualTo(DAYS);

        String range = "timestamp >= " + ts(end.minus(Duration.ofHours(1))) + " AND timestamp < " + ts(end);
        String lastHour = "SELECT metric_type, avg(value) FROM %s WHERE " + range + " GROUP BY metric_type";
        String oneSeries = "SELECT avg(value) FROM %s WHERE sensor_id = 's-00001' AND metric_type = 'voltage' AND " + range;

        assertThat(chunksInPlan(lastHour.formatted("metrics"))).hasSize(1);

        section("2. One B-tree over everything vs. one per chunk");
        report.append("| | Plain table | Hypertable |\n|---|---:|---:|\n");
        report.append("| heap | ").append(mb(plainHeap)).append(" | ")
                .append(mb(queryLong("SELECT table_bytes FROM hypertable_detailed_size('metrics')"))).append(" |\n");
        report.append("| indexes, total | ").append(mb(plainIndexes)).append(" | ").append(mb(hyperIndexes))
                .append(" (").append(chunks).append(" chunks) |\n");
        report.append("| index an insert of *now* touches | ").append(mb(plainIndexes)).append(" | ")
                .append(mb(newestChunkIndexes)).append(" (newest chunk) |\n");
        report.append("| relations planned for \"last hour, all sensors\" | 1 table, 2 indexes over all rows | ")
                .append(chunksInPlan(lastHour.formatted("metrics")).size()).append(" of ").append(chunks)
                .append(" chunks |\n\n");
        report.append("Median query time, ms (plain / hypertable):\n\n");
        report.append("- last hour, all sensors, `avg` per metric: ").append(fmt(median(lastHour.formatted("plain_metrics"))))
                .append(" / ").append(fmt(median(lastHour.formatted("metrics")))).append('\n');
        report.append("- last hour, one series: ").append(fmt(median(oneSeries.formatted("plain_metrics"))))
                .append(" / ").append(fmt(median(oneSeries.formatted("metrics")))).append('\n');
    }

    private void continuousAggregateVersusMaterializedView() {
        String aggregate = """
                SELECT time_bucket('5 minutes', "timestamp") AS bucket, sensor_id, metric_type,
                       avg(value) AS avg_value, min(value) AS min_value, max(value) AS max_value,
                       count(*) AS sample_count
                FROM metrics
                GROUP BY bucket, sensor_id, metric_type""";
        jdbc.execute("CREATE MATERIALIZED VIEW bench_5m_ca WITH (timescaledb.continuous, "
                + "timescaledb.materialized_only = false) AS " + aggregate + " WITH NO DATA");
        double caInitial = millis(() -> jdbc.execute("CALL refresh_continuous_aggregate('bench_5m_ca', NULL, NULL)"));
        double mvInitial = millis(() -> jdbc.execute("CREATE MATERIALIZED VIEW bench_5m_mv AS " + aggregate));

        jdbc.update(generate("metrics", end, end.plus(Duration.ofMinutes(5))));
        String fresh = "SELECT coalesce(sum(sample_count), 0) FROM %s WHERE bucket >= " + ts(end);
        long caFresh = queryLong(fresh.formatted("bench_5m_ca"));
        long mvFresh = queryLong(fresh.formatted("bench_5m_mv"));
        assertThat(caFresh).as("real-time aggregation folds in rows past the watermark").isEqualTo(SENSORS * 2L * 5);
        assertThat(mvFresh).as("a plain materialized view is a snapshot").isZero();

        double caWindow = millis(() -> jdbc.execute("CALL refresh_continuous_aggregate('bench_5m_ca', "
                + ts(end.minus(Duration.ofHours(1))) + ", " + ts(end.plus(Duration.ofHours(1))) + ")"));
        double mvFull = millis(() -> jdbc.execute("REFRESH MATERIALIZED VIEW bench_5m_mv"));

        Instant late = dayStart(DAYS - 1).plusSeconds(30);
        for (String table : List.of("metrics", "plain_metrics")) {
            jdbc.update("INSERT INTO " + table + " VALUES (" + ts(late) + ", 's-00001', 'voltage', 999, 'CRITICAL')");
        }
        String lateBucket = "SELECT max_value FROM bench_5m_ca WHERE sensor_id = 's-00001' AND metric_type = 'voltage' "
                + "AND bucket = time_bucket('5 minutes', " + ts(late) + ")";
        double beforeRefresh = jdbc.queryForObject(lateBucket, Double.class);
        double caInvalidated = millis(() -> jdbc.execute("CALL refresh_continuous_aggregate('bench_5m_ca', NULL, NULL)"));
        double afterRefresh = jdbc.queryForObject(lateBucket, Double.class);
        assertThat(beforeRefresh).isLessThan(999);
        assertThat(afterRefresh).isEqualTo(999);

        String lastDayRaw = "SELECT sensor_id, metric_type, max(value) FROM metrics WHERE timestamp >= "
                + ts(end.minus(Duration.ofDays(1))) + " GROUP BY sensor_id, metric_type";
        String lastDayCa = "SELECT sensor_id, metric_type, max(max_value) FROM bench_5m_ca WHERE bucket >= "
                + ts(end.minus(Duration.ofDays(1))) + " GROUP BY sensor_id, metric_type";

        section("3. Continuous aggregate vs. materialized view");
        report.append("Same 5-minute aggregate as `metrics_5m_avg` (`materialized_only = false`), as a continuous "
                + "aggregate and as a plain `CREATE MATERIALIZED VIEW`.\n\n");
        report.append("| Step | Continuous aggregate | Materialized view |\n|---|---:|---:|\n");
        report.append("| build over all ").append(DAYS).append(" days, ms | ").append(fmt(caInitial)).append(" | ")
                .append(fmt(mvInitial)).append(" |\n");
        report.append("| rows of 5 new minutes visible without a refresh | ").append(caFresh).append(" | ")
                .append(mvFresh).append(" |\n");
        report.append("| catch up after the new minutes, ms | ").append(fmt(caWindow))
                .append(" (2-hour window) | ").append(fmt(mvFull)).append(" (full `REFRESH`) |\n");
        report.append("| late row in an old bucket: `max` before → after refresh | ")
                .append(fmt(beforeRefresh)).append(" → ").append(fmt(afterRefresh)).append(" in ")
                .append(fmt(caInvalidated)).append(" ms (invalidated bucket only) | full `REFRESH` again |\n\n");
        report.append("Per-series max over the last day, median ms: raw hypertable ")
                .append(fmt(median(lastDayRaw))).append(" / continuous aggregate ").append(fmt(median(lastDayCa)))
                .append(".\n");
    }

    private void spacePartitioning() {
        jdbc.execute("""
                CREATE TABLE metrics_space (
                    timestamp   TIMESTAMPTZ      NOT NULL,
                    sensor_id   TEXT             NOT NULL,
                    metric_type TEXT             NOT NULL,
                    value       DOUBLE PRECISION NOT NULL,
                    status      TEXT             NOT NULL
                )""");
        jdbc.execute("SELECT create_hypertable('metrics_space', by_range('timestamp', INTERVAL '1 day'))");
        jdbc.execute("SELECT add_dimension('metrics_space', by_hash('sensor_id', 4))");
        jdbc.update("INSERT INTO metrics_space SELECT * FROM metrics WHERE timestamp >= "
                + ts(dayStart(DAYS - 2)) + " AND timestamp < " + ts(end));
        jdbc.execute("ANALYZE metrics_space");

        long chunks = queryLong("SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'metrics_space'");
        String oneSensor = "SELECT avg(value) FROM metrics_space WHERE sensor_id = 's-00001' AND timestamp >= "
                + ts(dayStart(DAYS - 2)) + " AND timestamp < " + ts(end);
        Set<String> scanned = chunksInPlan(oneSensor);
        assertThat(chunks).isEqualTo(2 * 4);
        assertThat(scanned).hasSize(2);

        List<String> dims = jdbc.query("""
                SELECT column_name, dimension_type, coalesce(time_interval::text, num_partitions::text)
                FROM timescaledb_information.dimensions WHERE hypertable_name = 'metrics_space'
                ORDER BY dimension_number""", (rs, n) -> "`" + rs.getString(1) + "` " + rs.getString(2).toLowerCase(Locale.ROOT)
                + " (" + rs.getString(3) + ")");

        section("4. Partitioning across time *and* space");
        report.append("Demo hypertable `metrics_space`: ").append(String.join(" × ", dims))
                .append(", loaded with the last 2 days.\n\n");
        report.append("- chunks: ").append(chunks).append(" (2 time slices × 4 hash partitions)\n");
        report.append("- one sensor over both days plans ").append(scanned.size()).append(" of ").append(chunks)
                .append(" chunks — time exclusion *and* hash exclusion on `sensor_id = ...`\n");
    }

    private void compression() {
        Instant cutoff = dayStart(DAYS - 3);
        String oneDay = "SELECT avg(value), max(value) FROM %s WHERE sensor_id = 's-00042' AND metric_type = 'cable_temperature' "
                + "AND timestamp >= " + ts(dayStart(1)) + " AND timestamp < " + ts(dayStart(2));
        String allSensorsDay = "SELECT metric_type, avg(value) FROM %s WHERE timestamp >= " + ts(dayStart(1))
                + " AND timestamp < " + ts(dayStart(2)) + " GROUP BY metric_type";
        double seriesBefore = median(oneDay.formatted("metrics"));
        double scanBefore = median(allSensorsDay.formatted("metrics"));
        long bytesBefore = queryLong("SELECT hypertable_size('metrics')");

        long compressed = jdbc.queryForList("SELECT compress_chunk(c) FROM show_chunks('metrics', older_than => "
                + ts(cutoff) + ") c", String.class).size();
        jdbc.execute("ANALYZE metrics");

        long bytesAfter = queryLong("SELECT hypertable_size('metrics')");
        Map<String, Object> stats = jdbc.queryForMap("""
                SELECT before_compression_total_bytes AS before, after_compression_total_bytes AS after
                FROM hypertable_compression_stats('metrics')""");
        long chunkBefore = ((Number) stats.get("before")).longValue();
        long chunkAfter = ((Number) stats.get("after")).longValue();
        assertThat(compressed).isEqualTo(DAYS - 3);
        assertThat(chunkBefore).isGreaterThan(chunkAfter);

        section("5. Columnar compression");
        report.append(String.format(Locale.ROOT,
                "`compress_chunk` on the %d chunks older than 3 days (segmentby `sensor_id, metric_type`, "
                        + "orderby `timestamp DESC`, as in `schema.sql`).%n%n", compressed));
        report.append("| | Before | After |\n|---|---:|---:|\n");
        report.append("| compressed chunks, data + indexes | ").append(mb(chunkBefore)).append(" | ").append(mb(chunkAfter))
                .append(String.format(Locale.ROOT, " (**%.1f×** smaller) |%n", (double) chunkBefore / chunkAfter));
        report.append("| whole hypertable | ").append(mb(bytesBefore)).append(" | ").append(mb(bytesAfter)).append(" |\n");
        report.append("| one series, one old day, median ms | ").append(fmt(seriesBefore)).append(" | ")
                .append(fmt(median(oneDay.formatted("metrics")))).append(" |\n");
        report.append("| every sensor, one old day, median ms | ").append(fmt(scanBefore)).append(" | ")
                .append(fmt(median(allSensorsDay.formatted("metrics")))).append(" |\n");
        report.append("| (same two queries on the plain table) | ").append(fmt(median(oneDay.formatted("plain_metrics"))))
                .append(" / ").append(fmt(median(allSensorsDay.formatted("plain_metrics")))).append(" | |\n");
    }

    private void tiering() throws Exception {
        ExecResult mkdir = POSTGRES.execInContainer("sh", "-c",
                "mkdir -p " + TABLESPACE_DIR + " && chown postgres:postgres " + TABLESPACE_DIR);
        assertThat(mkdir.getExitCode()).as(mkdir.getStderr()).isZero();
        jdbc.execute("CREATE TABLESPACE history LOCATION '" + TABLESPACE_DIR + "'");

        Instant cutoff = dayStart(DAYS - 6);
        long before = count("metrics", "timestamp < " + ts(end));
        List<String> old = jdbc.queryForList("SELECT c::text FROM show_chunks('metrics', older_than => " + ts(cutoff)
                + ") c", String.class);
        double moveMillis = millis(() -> old.forEach(chunk -> jdbc.execute("SELECT move_chunk(chunk => '" + chunk
                + "', destination_tablespace => 'history', index_destination_tablespace => 'history')")));

        long moved = queryLong("""
                SELECT count(*) FROM timescaledb_information.chunks
                WHERE hypertable_name = 'metrics' AND chunk_tablespace = 'history'""");
        assertThat(moved).isEqualTo(old.size()).isEqualTo(DAYS - 6);
        assertThat(count("metrics", "timestamp < " + ts(end))).as("moving chunks must not lose rows").isEqualTo(before);

        section("6. Tiering: old chunks to another tablespace");
        report.append(String.format(Locale.ROOT,
                "`move_chunk(..., destination_tablespace => 'history', index_destination_tablespace => 'history')` on "
                        + "the %d chunks older than 6 days: %s ms in total; rows before = after = %,d.%n%n",
                old.size(), fmt(moveMillis), before));
        report.append("Lifecycle of `metrics` after this step:\n\n| Chunk range | Compressed | Tablespace |\n|---|---|---|\n");
        jdbc.query("""
                SELECT range_start, range_end, is_compressed, coalesce(chunk_tablespace, 'pg_default') AS ts
                FROM timescaledb_information.chunks WHERE hypertable_name = 'metrics' ORDER BY range_start""",
                rs -> {
                    report.append("| ").append(rs.getTimestamp(1).toInstant().toString(), 0, 10)
                            .append(rs.getTimestamp(2).toInstant().isAfter(end) ? " (today)" : "")
                            .append(" | ").append(rs.getBoolean(3) ? "yes" : "no")
                            .append(" | ").append(rs.getString(4)).append(" |\n");
                });
    }

    private void retention() {
        Instant cutoff = dayStart(2);
        double dropMillis = millis(() -> jdbc.queryForList("SELECT drop_chunks('metrics', older_than => " + ts(cutoff) + ")",
                String.class));
        double deleteMillis = millis(() -> jdbc.update("DELETE FROM plain_metrics WHERE timestamp < " + ts(cutoff)));
        double vacuumMillis = millis(() -> jdbc.execute("VACUUM plain_metrics"));

        String kept = "timestamp < " + ts(end);
        assertThat(count("metrics", kept)).isEqualTo(count("plain_metrics", kept));
        assertThat(count("metrics", "timestamp < " + ts(cutoff))).isZero();

        section("7. Retention: dropping the 2 oldest days");
        report.append("| | Plain table | Hypertable |\n|---|---:|---:|\n");
        report.append("| remove rows, ms | ").append(fmt(deleteMillis)).append(" (`DELETE`) | ").append(fmt(dropMillis))
                .append(" (`drop_chunks`) |\n");
        report.append("| reclaim space / index entries, ms | ").append(fmt(vacuumMillis))
                .append(" (`VACUUM`) | — (files unlinked) |\n");
    }

    private static String generate(String table, Instant from, Instant to) {
        return """
                INSERT INTO %s (timestamp, sensor_id, metric_type, value, status)
                SELECT t, 's-' || lpad(s::text, 5, '0'), m.metric_type,
                       round((m.base + m.amp * sin(extract(epoch FROM t) / 3600.0 + s) + random() * m.noise)::numeric, 2)::float8,
                       'OK'
                FROM generate_series(%s, %s - INTERVAL '1 minute', INTERVAL '1 minute') t
                CROSS JOIN generate_series(1, %d) s
                CROSS JOIN (VALUES ('voltage', 230.0, 3.0, 0.5), ('cable_temperature', 60.0, 8.0, 1.0))
                    AS m(metric_type, base, amp, noise)""".formatted(table, ts(from), ts(to), SENSORS);
    }

    private static Instant dayStart(int day) {
        return end.minus(Duration.ofDays(DAYS - day));
    }

    private static String ts(Instant instant) {
        return "'" + instant + "'::timestamptz";
    }

    private static long count(String table, String where) {
        return queryLong("SELECT count(*) FROM " + table + " WHERE " + where);
    }

    private static long queryLong(String sql) {
        return jdbc.queryForObject(sql, Long.class);
    }

    private static Set<String> chunksInPlan(String sql) {
        Set<String> chunks = new TreeSet<>();
        for (String line : jdbc.queryForList("EXPLAIN " + sql, String.class)) {
            Matcher m = CHUNK.matcher(line);
            while (m.find()) {
                chunks.add(m.group());
            }
        }
        return chunks;
    }

    private static double millis(Runnable work) {
        long t = System.nanoTime();
        work.run();
        return (System.nanoTime() - t) / 1e6;
    }

    private static double median(String sql) {
        for (int i = 0; i < WARMUP; i++) {
            jdbc.queryForList(sql);
        }
        double[] times = new double[RUNS];
        for (int i = 0; i < RUNS; i++) {
            times[i] = millis(() -> jdbc.queryForList(sql));
        }
        Arrays.sort(times);
        return times[RUNS / 2];
    }

    private void section(String title) {
        report.append("\n## ").append(title).append("\n\n");
    }

    private String header() {
        Map<String, String> env = new LinkedHashMap<>();
        env.put("image", IMAGE.asCanonicalNameString() + " (Testcontainers)");
        env.put("data", SENSORS + " sensors × 2 metrics × 1 reading/min × " + DAYS + " days, ending " + end);
        env.put("schema", "`metrics` from `schema.sql` (1-day chunks, policies removed); `plain_metrics` = same "
                + "columns and indexes as a heap table");
        env.put("timing", "single wall-clock run for DDL/DML; queries = median of " + RUNS + " after " + WARMUP + " warm-up");
        StringBuilder sb = new StringBuilder("# Time-series storage: plain PostgreSQL vs TimescaleDB — benchmark run\n\n");
        env.forEach((k, v) -> sb.append("- **").append(k).append("**: ").append(v).append('\n'));
        return sb.toString();
    }

    private static String fmt(double v) {
        return String.format(Locale.ROOT, "%.2f", v);
    }

    private static String mb(long bytes) {
        return String.format(Locale.ROOT, "%.1f MB", bytes / 1048576.0);
    }
}
