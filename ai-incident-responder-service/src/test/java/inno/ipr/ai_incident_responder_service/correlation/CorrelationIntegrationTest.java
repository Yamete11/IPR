package inno.ipr.ai_incident_responder_service.correlation;

import com.zaxxer.hikari.HikariDataSource;
import inno.ipr.ai_incident_responder_service.age.AgtypeParser;
import inno.ipr.ai_incident_responder_service.ai.MockIncidentPlanner;
import inno.ipr.ai_incident_responder_service.consumer.AlertConsumer;
import inno.ipr.ai_incident_responder_service.domain.MetricSummary;
import inno.ipr.ai_incident_responder_service.repository.GraphRepository;
import inno.ipr.ai_incident_responder_service.repository.IncidentRepository;
import inno.ipr.ai_incident_responder_service.repository.MetricHistoryRepository;
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
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Root-cause correlation end to end below Kafka: the real {@link AlertConsumer}, {@link GraphRepository}
 * on AGE holding the hand-seeded city, {@link IncidentRepository} and {@link IncidentCorrelator} on
 * TimescaleDB, and the offline planner as a spy counting would-be Claude calls. Only the metric
 * history is mocked.
 *
 * <p>The city: voltage sensor {@code s-00311} watches {@code sub-1}; pressure sensor {@code s-00042}
 * watches {@code node-5}, which {@code sub-1} feeds. {@code causeFirst}: the substation alert is planned,
 * the node alert becomes its child with no planner call. {@code effectFirst}: the node alert arrives
 * first and is planned as a root; the substation alert, planned too, adopts it.
 *
 * <p>Self-skips when Docker is unavailable so {@code ./gradlew build} still passes without it.
 */
@Testcontainers
@EnabledIf("inno.ipr.ai_incident_responder_service.support.Containers#dockerAvailable")
class CorrelationIntegrationTest {

    private static final Instant T = Instant.parse("2026-11-08T12:00:00Z");

    @Container
    private static final PostgreSQLContainer AGE = Containers.age();

    @Container
    private static final PostgreSQLContainer TIMESCALE = Containers.timescale();

    private static HikariDataSource ageDataSource;
    private static HikariDataSource timescaleDataSource;
    private static JdbcTemplate timescale;
    private MockIncidentPlanner planner;
    private AlertConsumer consumer;

    @BeforeAll
    static void databases() {
        ageDataSource = Containers.ageDataSource(AGE);
        Containers.seedCity(new JdbcTemplate(ageDataSource));
        timescaleDataSource = Containers.timescaleDataSource(TIMESCALE);
        Containers.applyIncidentsSchema(timescaleDataSource);
        timescale = new JdbcTemplate(timescaleDataSource);
    }

    @AfterAll
    static void close() {
        if (ageDataSource != null) {
            ageDataSource.close();
        }
        if (timescaleDataSource != null) {
            timescaleDataSource.close();
        }
    }

    @BeforeEach
    void wire() {
        timescale.update("DELETE FROM incidents");
        JsonMapper mapper = JsonMapper.builder().build();
        IncidentRepository incidents = new IncidentRepository(timescale);
        MetricHistoryRepository metrics = mock(MetricHistoryRepository.class);
        given(metrics.findLastHour(anyString(), anyString(), any()))
                .willReturn(new MetricSummary(0, null, null, null, null, null));
        planner = spy(new MockIncidentPlanner());
        consumer = new AlertConsumer(mapper, incidents, metrics,
                new GraphRepository(new JdbcTemplate(ageDataSource), new AgtypeParser(mapper), mapper),
                planner, new IncidentCorrelator(incidents, timescaleDataSource, true, Duration.ofMinutes(30)));
    }

    @Test
    @DisplayName("cause first: the downstream alert is filed under the substation incident without a plan")
    void causeFirst() {
        consumer.onMessage(alert("a-sub", "s-00311", "substation_voltage", T));
        consumer.onMessage(alert("a-node", "s-00042", "pipe_pressure", T.plusSeconds(10)));

        verify(planner, times(1)).plan(any());
        Map<String, Object> root = row("a-sub");
        Map<String, Object> child = row("a-node");
        assertThat(root).containsEntry("asset_id", "sub-1").containsEntry("root_incident_id", null)
                .containsEntry("model", "mock");
        assertThat(child).containsEntry("asset_id", "node-5").containsEntry("model", "correlated")
                .containsEntry("root_incident_id", root.get("incident_id"));
        assertThat((String) root.get("crews")).contains("crew-hv", "crew-grid", "crew-north", "crew-south");
        assertThat((String) child.get("crews")).contains("crew-grid", "crew-north").doesNotContain("crew-hv");
    }

    @Test
    @DisplayName("effect first: the substation incident, planned later, adopts the node incident")
    void effectFirst() {
        consumer.onMessage(alert("a-node", "s-00042", "pipe_pressure", T));
        consumer.onMessage(alert("a-sub", "s-00311", "substation_voltage", T.plusSeconds(10)));

        verify(planner, times(2)).plan(any());
        Map<String, Object> root = row("a-sub");
        assertThat(root).containsEntry("root_incident_id", null);
        assertThat(row("a-node")).containsEntry("root_incident_id", root.get("incident_id"))
                .containsEntry("model", "mock");
    }

    @Test
    @DisplayName("a second sensor on the same asset joins the open incident")
    void sameAssetJoins() {
        consumer.onMessage(alert("a-volt", "s-00311", "substation_voltage", T));
        consumer.onMessage(alert("a-cable", "s-00108", "cable_temperature", T.plusSeconds(5)));

        verify(planner, times(1)).plan(any());
        assertThat(row("a-cable")).containsEntry("root_incident_id", row("a-volt").get("incident_id"));
    }

    private static Map<String, Object> row(String alertId) {
        return timescale.queryForMap("SELECT incident_id, asset_id, root_incident_id, model, crews::text AS crews "
                + "FROM incidents WHERE alert_id = ?", alertId);
    }

    private static String alert(String alertId, String sensorId, String metric, Instant detectedAt) {
        return """
                {"alert_id":"%s","sensor_id":"%s","metric_type":"%s","value":0.0,"detected_at":"%s","rule":"r"}"""
                .formatted(alertId, sensorId, metric, detectedAt);
    }
}
