package inno.ipr.anomaly_detector_service.detection;

import inno.ipr.anomaly_detector_service.config.KafkaConfig;
import inno.ipr.anomaly_detector_service.domain.MetricWindow;
import inno.ipr.anomaly_detector_service.repository.MetricAggregateRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

/**
 * The scheduler → rules → publisher → broker loop, end to end, with only the aggregate query mocked.
 * Proves the edge trigger across real polls: one alert per fault no matter how many ticks see it,
 * silence while the series is healthy, and a fresh alert on the next breach.
 *
 * <p>The datasource is excluded (the aggregate repository is mocked and the continuous-aggregate
 * initializer is switched off), and the poll interval is shortened so the test observes several
 * ticks per second. With no datasource there is no {@code db} health contributor, so the readiness
 * group includes only {@code readinessState}.
 *
 * <p>{@code edgeTriggeredAcrossPolls}: several ticks (at least 5 at 200 ms) see the same standing
 * fault and produce exactly one alert; on recovery the series clears and nothing is published; a
 * second breach is a new rising edge and produces exactly one more alert, with a new id.
 *
 * <p>{@code collectAlerts(String, Duration)} returns every alert for {@code sensor} that arrives
 * within {@code window}, as parsed JSON maps.
 */
@SpringBootTest(properties = {
        "spring.autoconfigure.exclude=org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
        "anomaly.schema.init-enabled=false",
        "anomaly.poll-interval-ms=200",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "management.endpoint.health.group.readiness.include=readinessState",
})
@EmbeddedKafka(partitions = 1)
class DetectionPipelineIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private MetricAggregateRepository repository;

    private Consumer<String, String> alerts;

    @BeforeEach
    void setUp() {
        Map<String, Object> props = KafkaTestUtils.consumerProps(broker, "alerts-" + UUID.randomUUID(), false);
        alerts = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
        alerts.subscribe(List.of(KafkaConfig.ALERTS_TOPIC));
    }

    @AfterEach
    void tearDown() {
        alerts.close(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("one alert per fault across many polls, then a new alert after recovery")
    void edgeTriggeredAcrossPolls() {
        String sensor = "s-" + UUID.randomUUID();
        MetricWindow faulted = new MetricWindow(sensor, "pipe_pressure", Instant.now(), 0.0, 4.0);
        MetricWindow healthy = new MetricWindow(sensor, "pipe_pressure", Instant.now(), 3.6, 4.4);

        given(repository.findRecentWindows(any())).willReturn(List.of(faulted));

        List<Map<String, Object>> first = collectAlerts(sensor, Duration.ofSeconds(3));
        assertThat(first).hasSize(1);
        assertThat(first.get(0))
                .containsEntry("sensor_id", sensor)
                .containsEntry("metric_type", "pipe_pressure")
                .containsEntry("value", 0.0)
                .containsEntry("rule", "pipe_pressure_min<0.5");
        assertThat((String) first.get(0).get("alert_id")).startsWith("a-");
        assertThat(first.get(0)).containsKey("detected_at");

        given(repository.findRecentWindows(any())).willReturn(List.of(healthy));
        assertThat(collectAlerts(sensor, Duration.ofSeconds(2))).isEmpty();

        given(repository.findRecentWindows(any())).willReturn(List.of(faulted));
        List<Map<String, Object>> second = collectAlerts(sensor, Duration.ofSeconds(3));
        assertThat(second).hasSize(1);
        assertThat(second.get(0).get("alert_id")).isNotEqualTo(first.get(0).get("alert_id"));
    }

    @Test
    @DisplayName("an unmonitored metric never produces an alert")
    void unmonitoredMetricIsSilent() {
        String sensor = "s-" + UUID.randomUUID();
        given(repository.findRecentWindows(any()))
                .willReturn(List.of(new MetricWindow(sensor, "humidity", Instant.now(), 0, 0)));

        assertThat(collectAlerts(sensor, Duration.ofSeconds(2))).isEmpty();
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> collectAlerts(String sensor, Duration window) {
        List<Map<String, Object>> found = new ArrayList<>();
        long deadline = System.nanoTime() + window.toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : alerts.poll(Duration.ofMillis(200))) {
                if (sensor.equals(record.key())) {
                    found.add(objectMapper.readValue(record.value(), Map.class));
                }
            }
        }
        return found;
    }
}
