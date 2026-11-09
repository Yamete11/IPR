package inno.ipr.ai_incident_responder_service.repository;

import com.zaxxer.hikari.HikariDataSource;
import inno.ipr.ai_incident_responder_service.domain.Incident;
import inno.ipr.ai_incident_responder_service.repository.IncidentRepository.RootIncident;
import inno.ipr.ai_incident_responder_service.support.Containers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link IncidentRepository}'s correlation SQL against real TimescaleDB, on a table migrated the way
 * production's is: the pre-correlation DDL first, then this service's {@code schema.sql} twice (its
 * {@code ALTER ... IF NOT EXISTS} must upgrade an old table and be idempotent).
 *
 * <p>Covers the window in both directions and its edges, the upstream-first ranking, that a child is
 * never returned as a root, that {@code adopt} moves a root and its children together (groups stay one
 * level deep), and the crews JSONB round trip.
 *
 * <p>Self-skips when Docker is unavailable so {@code ./gradlew build} still passes without it.
 */
@Testcontainers
@EnabledIf("inno.ipr.ai_incident_responder_service.support.Containers#dockerAvailable")
class IncidentRepositoryIntegrationTest {

    private static final Instant T = Instant.parse("2026-11-08T12:00:00Z");
    private static final Duration WINDOW = Duration.ofMinutes(30);

    @Container
    private static final PostgreSQLContainer TIMESCALE = Containers.timescale();

    private static HikariDataSource dataSource;
    private static JdbcTemplate jdbc;
    private static IncidentRepository incidents;

    @BeforeAll
    static void migrate() {
        dataSource = Containers.timescaleDataSource(TIMESCALE);
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("""
                CREATE TABLE incidents (
                    incident_id TEXT PRIMARY KEY, alert_id TEXT NOT NULL UNIQUE, sensor_id TEXT NOT NULL,
                    metric_type TEXT, criticality TEXT NOT NULL, impacted_objects JSONB NOT NULL,
                    remediation_steps JSONB NOT NULL, summary TEXT, model TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now())""");
        jdbc.update("INSERT INTO incidents (incident_id, alert_id, sensor_id, criticality, impacted_objects, "
                + "remediation_steps) VALUES ('i-old', 'a-old', 's-1', 'LOW', '[]', '[]')");
        Containers.applyIncidentsSchema(dataSource);
        Containers.applyIncidentsSchema(dataSource);
        incidents = new IncidentRepository(jdbc);
    }

    @AfterAll
    static void close() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @BeforeEach
    void clean() {
        jdbc.update("DELETE FROM incidents WHERE incident_id <> 'i-old'");
    }

    @Test
    @DisplayName("the migration keeps old rows and gives them empty crews")
    void migrationKeepsOldRows() {
        Map<String, Object> old = jdbc.queryForMap(
                "SELECT asset_id, root_incident_id, crews::text AS crews FROM incidents WHERE incident_id = 'i-old'");

        assertThat(old).containsEntry("asset_id", null).containsEntry("root_incident_id", null)
                .containsEntry("crews", "[]");
    }

    @Test
    @DisplayName("a root within ±window is found in both directions; outside it is not")
    void windowInBothDirections() {
        incidents.insert(incident("i-sub", "sub-1", null, T));

        assertThat(incidents.findActiveRoot(List.of("sub-1", "node-5"), T.plus(WINDOW), WINDOW)).isPresent();
        assertThat(incidents.findActiveRoot(List.of("sub-1", "node-5"), T.minus(WINDOW), WINDOW)).isPresent();
        assertThat(incidents.findActiveRoot(List.of("sub-1", "node-5"), T.plus(WINDOW).plusSeconds(1), WINDOW))
                .isEmpty();
        assertThat(incidents.findActiveRoot(List.of("node-6"), T, WINDOW)).isEmpty();
    }

    @Test
    @DisplayName("the most upstream root wins, and a child is never returned as a root")
    void rankingAndChildren() {
        incidents.insert(incident("i-node", "node-5", null, T));
        incidents.insert(incident("i-sub", "sub-1", null, T.plusSeconds(60)));
        incidents.insert(incident("i-child", "bld-12", "i-sub", T));

        assertThat(incidents.findActiveRoot(List.of("sub-1", "node-5", "bld-12"), T, WINDOW))
                .contains(new RootIncident("i-sub", "HIGH", "sub-1"));
        assertThat(incidents.findActiveRoot(List.of("bld-12"), T, WINDOW)).isEmpty();
    }

    @Test
    @DisplayName("adopt moves earlier downstream roots and their children under the new root, one level deep")
    void adoptFlattens() {
        incidents.insert(incident("i-node", "node-5", null, T));
        incidents.insert(incident("i-bld", "bld-12", "i-node", T));
        incidents.insert(incident("i-far", "node-6", null, T.minus(Duration.ofHours(2))));
        incidents.insert(incident("i-sub", "sub-1", null, T.plusSeconds(30)));

        int moved = incidents.adopt("i-sub", List.of("sub-1", "node-5", "node-6", "bld-12"), T.plusSeconds(30), WINDOW);

        assertThat(moved).isEqualTo(2);
        assertThat(rootOf("i-node")).isEqualTo("i-sub");
        assertThat(rootOf("i-bld")).isEqualTo("i-sub");
        assertThat(rootOf("i-far")).as("outside the window").isNull();
        assertThat(rootOf("i-sub")).isNull();
    }

    @Test
    @DisplayName("a duplicate alert inserts nothing; crews survive the JSONB round trip")
    void insertIsIdempotentAndKeepsCrews() {
        Incident incident = incident("i-a", "node-5", null, T);

        assertThat(incidents.insert(incident)).isEqualTo(1);
        assertThat(incidents.insert(incident)).isZero();
        assertThat(incidents.existsByAlertId("a-i-a")).isTrue();
        assertThat(jdbc.queryForObject("SELECT crews->0->>'name' FROM incidents WHERE incident_id = 'i-a'",
                String.class)).isEqualTo("Distribution grid crew");
    }

    private static String rootOf(String incidentId) {
        return jdbc.queryForObject("SELECT root_incident_id FROM incidents WHERE incident_id = ?",
                String.class, incidentId);
    }

    private static Incident incident(String id, String asset, String root, Instant detectedAt) {
        return new Incident(id, "a-" + id, "s-1", "pipe_pressure", asset, root, detectedAt, "HIGH", "[]", "[]",
                "[{\"crew_id\":\"crew-grid\",\"name\":\"Distribution grid crew\",\"phone\":null,\"assets\":[]}]",
                "summary", "mock");
    }
}
