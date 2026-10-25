package inno.ipr.incident_api_service.repository;

import com.zaxxer.hikari.HikariDataSource;
import inno.ipr.incident_api_service.graphql.dto.IncidentDto;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The read repository against a real TimescaleDB with the {@code incidents} table exactly as
 * ai-incident-responder-service creates it. What matters here is the part no mock can cover: the
 * JSONB array columns coming back as text and being parsed into lists, plus ordering and the
 * {@code LIMIT} on the per-sensor query.
 *
 * <p>{@code IMAGE} matches the image pinned in the repo's docker-compose.yml, and
 * {@code INCIDENTS_DDL} is a copy (without comments) of
 * {@code ai-incident-responder-service/src/main/resources/schema.sql}.
 *
 * <p>Self-skips when Docker is unavailable so {@code ./gradlew build} still passes without it.
 */
@Testcontainers
@EnabledIf("dockerAvailable")
class IncidentReadRepositoryIntegrationTest {

    private static final DockerImageName IMAGE = DockerImageName
            .parse("timescale/timescaledb:2.28.3-pg16")
            .asCompatibleSubstituteFor("postgres");

    private static final String INCIDENTS_DDL = """
            CREATE TABLE IF NOT EXISTS incidents (
                incident_id       TEXT PRIMARY KEY,
                alert_id          TEXT NOT NULL UNIQUE,
                sensor_id         TEXT NOT NULL,
                metric_type       TEXT,
                criticality       TEXT NOT NULL,
                impacted_objects  JSONB NOT NULL,
                remediation_steps JSONB NOT NULL,
                summary           TEXT,
                model             TEXT,
                created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """;

    private static final String INSERT = """
            INSERT INTO incidents (incident_id, alert_id, sensor_id, metric_type, criticality,
                                   impacted_objects, remediation_steps, summary, model, created_at)
            VALUES (?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?, now() + (? || ' seconds')::interval)
            """;

    @Container
    @SuppressWarnings("resource")
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(IMAGE)
            .withDatabaseName("smartcity")
            .withUsername("smartcity")
            .withPassword("smartcity");

    static boolean dockerAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    private static HikariDataSource dataSource;
    private static JdbcTemplate jdbc;
    private static IncidentReadRepository repository;

    @BeforeAll
    static void connect() {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(POSTGRES.getJdbcUrl());
        dataSource.setUsername(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        dataSource.setMaximumPoolSize(2);
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute(INCIDENTS_DDL);
        repository = new IncidentReadRepository(jdbc, JsonMapper.builder().build());
    }

    @AfterAll
    static void disconnect() {
        dataSource.close();
    }

    @BeforeEach
    void clean() {
        jdbc.execute("DELETE FROM incidents");
    }

    private static void insert(String id, String alertId, String sensor, String impacted, String steps, int secondsFromNow) {
        jdbc.update(INSERT, id, alertId, sensor, "pipe_pressure", "HIGH", impacted, steps,
                "summary " + id, "mock", String.valueOf(secondsFromNow));
    }

    @Test
    @DisplayName("JSONB arrays come back as lists and created_at as an ISO instant")
    void mapsJsonbColumns() {
        insert("i-1", "a-1", "s-00042",
                "[\"DistributionNode node-5\",\"ResidentialBuilding bld-12\"]",
                "[\"Dispatch a crew.\",\"Confirm restoration.\"]", 0);

        IncidentDto dto = repository.findById("i-1");

        assertThat(dto).isNotNull();
        assertThat(dto.impactedObjects()).containsExactly("DistributionNode node-5", "ResidentialBuilding bld-12");
        assertThat(dto.remediationSteps()).containsExactly("Dispatch a crew.", "Confirm restoration.");
        assertThat(dto.createdAt()).matches("\\d{4}-\\d{2}-\\d{2}T.*Z");
        assertThat(dto.model()).isEqualTo("mock");
        assertThat(repository.findById("i-missing")).isNull();
    }

    @Test
    @DisplayName("findRecent is newest first and honours its limit")
    void findRecentOrdersAndLimits() {
        insert("i-old", "a-old", "s-00042", "[]", "[]", -30);
        insert("i-mid", "a-mid", "s-00108", "[]", "[]", -20);
        insert("i-new", "a-new", "s-00042", "[]", "[]", -10);

        assertThat(repository.findRecent(50)).extracting(IncidentDto::incidentId).containsExactly("i-new", "i-mid", "i-old");
        assertThat(repository.findRecent(2)).extracting(IncidentDto::incidentId).containsExactly("i-new", "i-mid");
        assertThat(repository.findRecent(0)).isEmpty();
    }

    @Test
    @DisplayName("findBySensor filters by sensor, newest first, and is bounded by its limit")
    void findBySensorFiltersOrdersAndLimits() {
        insert("i-1", "a-1", "s-00042", "[]", "[]", -30);
        insert("i-2", "a-2", "s-00108", "[]", "[]", -20);
        insert("i-3", "a-3", "s-00042", "[]", "[]", -10);
        insert("i-4", "a-4", "s-00042", "[]", "[]", -5);

        List<IncidentDto> all = repository.findBySensor("s-00042", 50);
        assertThat(all).extracting(IncidentDto::incidentId).containsExactly("i-4", "i-3", "i-1");
        assertThat(repository.findBySensor("s-00042", 2)).extracting(IncidentDto::incidentId).containsExactly("i-4", "i-3");
        assertThat(repository.findBySensor("s-none", 50)).isEmpty();
    }

    @Test
    @DisplayName("an empty JSONB array is an empty list, and a non-array is a loud failure")
    void arrayEdgeCases() {
        insert("i-empty", "a-empty", "s-00042", "[]", "[]", 0);
        assertThat(repository.findById("i-empty").impactedObjects()).isEmpty();

        insert("i-bad", "a-bad", "s-00042", "{\"not\":\"an array\"}", "[]", 0);
        assertThatThrownBy(() -> repository.findById("i-bad")).isInstanceOf(IllegalStateException.class);
    }
}
