package inno.ipr.anomaly_detector_service.alert;

import inno.ipr.anomaly_detector_service.config.KafkaConfig;
import inno.ipr.anomaly_detector_service.detection.ThresholdRule;
import inno.ipr.anomaly_detector_service.domain.SeriesKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * The edge trigger and its re-arm rules. The subtle cases are the failures: a send that fails after
 * the key was armed must disarm it, or a standing fault is silenced for as long as it stands.
 *
 * <p>{@code failedSendReArms}: the series stays armed while the send is in flight and is disarmed
 * the moment the broker reports the loss.
 *
 * <p>{@code serialisationFailureReArms}: a healthy mapper cannot be swapped into the same
 * publisher, so the test asserts that the key was released through the failure repeating on the
 * next attempt instead of being swallowed as a "duplicate".
 *
 * <p>{@code staleFailureKeepsNewerAlertArmed}: alert A is still in flight when the series recovers
 * and breaches again as alert B; A's late failure must leave B's key in place.
 */
class AlertPublisherTest {

    private static final ThresholdRule.Breach BREACH = new ThresholdRule.Breach(0.0, "pipe_pressure_min<0.5");
    private static final SeriesKey PRESSURE_42 = new SeriesKey("s-00042", "pipe_pressure");
    private static final SeriesKey TEMPERATURE_42 = new SeriesKey("s-00042", "cable_temperature");
    private static final SeriesKey PRESSURE_108 = new SeriesKey("s-00108", "pipe_pressure");

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private final ObjectMapper objectMapper = JsonMapper.builder().build();
    private AlertPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new AlertPublisher(kafka, objectMapper);
    }

    @Test
    @DisplayName("fires once on the rising edge and suppresses the standing fault afterwards")
    void firesOnlyOnTheRisingEdge() {
        given(kafka.send(anyString(), anyString(), anyString())).willReturn(delivered());

        assertThat(publisher.publishIfNew(PRESSURE_42, BREACH)).isTrue();
        assertThat(publisher.publishIfNew(PRESSURE_42, BREACH)).isFalse();
        assertThat(publisher.publishIfNew(PRESSURE_42, BREACH)).isFalse();

        verify(kafka, times(1)).send(eq(KafkaConfig.ALERTS_TOPIC), eq("s-00042"), anyString());
    }

    @Test
    @DisplayName("the alert on the wire is keyed by sensor and carries the snake_case contract fields")
    void publishesTheContractShape() {
        given(kafka.send(anyString(), anyString(), anyString())).willReturn(delivered());

        publisher.publishIfNew(PRESSURE_42, BREACH);

        ArgumentCaptor<String> json = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq(KafkaConfig.ALERTS_TOPIC), eq("s-00042"), json.capture());
        assertThat(json.getValue())
                .contains("\"alert_id\":\"a-")
                .contains("\"sensor_id\":\"s-00042\"")
                .contains("\"metric_type\":\"pipe_pressure\"")
                .contains("\"value\":0.0")
                .contains("\"detected_at\":")
                .contains("\"rule\":\"pipe_pressure_min<0.5\"");
    }

    @Test
    @DisplayName("series are independent: one sensor's standing fault does not suppress another's")
    void keysAreScopedToTheSeries() {
        given(kafka.send(anyString(), anyString(), anyString())).willReturn(delivered());

        assertThat(publisher.publishIfNew(PRESSURE_42, BREACH)).isTrue();
        assertThat(publisher.publishIfNew(TEMPERATURE_42, BREACH)).isTrue();
        assertThat(publisher.publishIfNew(PRESSURE_108, BREACH)).isTrue();
    }

    @Test
    @DisplayName("recovery re-arms the series so the next breach fires again")
    void clearReArms() {
        given(kafka.send(anyString(), anyString(), anyString())).willReturn(delivered());

        publisher.publishIfNew(PRESSURE_42, BREACH);
        publisher.clear(PRESSURE_42);

        assertThat(publisher.publishIfNew(PRESSURE_42, BREACH)).isTrue();
        verify(kafka, times(2)).send(anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("a delivery failure disarms the series so the next poll retries the alert")
    void failedSendReArms() {
        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        given(kafka.send(anyString(), anyString(), anyString())).willReturn(failed, delivered());

        assertThat(publisher.publishIfNew(PRESSURE_42, BREACH)).isTrue();
        assertThat(publisher.publishIfNew(PRESSURE_42, BREACH)).isFalse();

        failed.completeExceptionally(new RuntimeException("broker unreachable"));

        assertThat(publisher.publishIfNew(PRESSURE_42, BREACH)).isTrue();
        verify(kafka, times(2)).send(anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("a late delivery failure of an old alert does not disarm a newer breach")
    void staleFailureKeepsNewerAlertArmed() {
        CompletableFuture<SendResult<String, String>> first = new CompletableFuture<>();
        given(kafka.send(anyString(), anyString(), anyString())).willReturn(first, delivered());

        publisher.publishIfNew(PRESSURE_42, BREACH);
        publisher.clear(PRESSURE_42);
        assertThat(publisher.publishIfNew(PRESSURE_42, BREACH)).isTrue();

        first.completeExceptionally(new RuntimeException("delivery timeout"));

        assertThat(publisher.publishIfNew(PRESSURE_42, BREACH)).isFalse();
    }

    @Test
    @DisplayName("a synchronous send failure disarms the series and propagates")
    void synchronousSendFailureReArms() {
        given(kafka.send(anyString(), anyString(), anyString()))
                .willThrow(new IllegalStateException("producer closed"))
                .willReturn(delivered());

        assertThatThrownBy(() -> publisher.publishIfNew(PRESSURE_42, BREACH))
                .isInstanceOf(IllegalStateException.class);
        assertThat(publisher.publishIfNew(PRESSURE_42, BREACH)).isTrue();
    }

    @Test
    @DisplayName("a series that stops reporting is disarmed, so its next fault fires")
    void silentSeriesIsDisarmed() {
        given(kafka.send(anyString(), anyString(), anyString())).willReturn(delivered());

        publisher.publishIfNew(PRESSURE_42, BREACH);
        publisher.publishIfNew(PRESSURE_108, BREACH);
        publisher.retainOnly(Set.of(PRESSURE_108));

        assertThat(publisher.publishIfNew(PRESSURE_42, BREACH)).isTrue();
        assertThat(publisher.publishIfNew(PRESSURE_108, BREACH)).isFalse();
    }

    @Test
    @DisplayName("a serialisation failure disarms the series and propagates")
    void serialisationFailureReArms() {
        ObjectMapper broken = mock(ObjectMapper.class);
        given(broken.writeValueAsString(any())).willThrow(new IllegalStateException("cannot serialise"));
        AlertPublisher withBrokenMapper = new AlertPublisher(kafka, broken);

        assertThatThrownBy(() -> withBrokenMapper.publishIfNew(PRESSURE_42, BREACH))
                .isInstanceOf(IllegalStateException.class);
        verify(kafka, never()).send(anyString(), anyString(), anyString());

        assertThatThrownBy(() -> withBrokenMapper.publishIfNew(PRESSURE_42, BREACH))
                .isInstanceOf(IllegalStateException.class);
    }

    private static CompletableFuture<SendResult<String, String>> delivered() {
        return CompletableFuture.completedFuture(null);
    }
}
