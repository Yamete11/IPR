package inno.ipr.anomaly_detector_service.alert;

import inno.ipr.anomaly_detector_service.config.KafkaConfig;
import inno.ipr.anomaly_detector_service.detection.ThresholdRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * The edge trigger and its re-arm rules. The subtle case is the last one: a send that fails after
 * the key was armed must disarm it, or a standing fault is silenced for as long as it stands.
 */
class AlertPublisherTest {

    private static final ThresholdRule.Breach BREACH = new ThresholdRule.Breach(0.0, "pipe_pressure_min<0.5");

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

        assertThat(publisher.publishIfNew("s-00042", "pipe_pressure", BREACH)).isTrue();
        assertThat(publisher.publishIfNew("s-00042", "pipe_pressure", BREACH)).isFalse();
        assertThat(publisher.publishIfNew("s-00042", "pipe_pressure", BREACH)).isFalse();

        verify(kafka, times(1)).send(eq(KafkaConfig.ALERTS_TOPIC), eq("s-00042"), anyString());
    }

    @Test
    @DisplayName("the alert on the wire is keyed by sensor and carries the snake_case contract fields")
    void publishesTheContractShape() {
        given(kafka.send(anyString(), anyString(), anyString())).willReturn(delivered());

        publisher.publishIfNew("s-00042", "pipe_pressure", BREACH);

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

        assertThat(publisher.publishIfNew("s-00042", "pipe_pressure", BREACH)).isTrue();
        assertThat(publisher.publishIfNew("s-00042", "cable_temperature", BREACH)).isTrue();
        assertThat(publisher.publishIfNew("s-00108", "pipe_pressure", BREACH)).isTrue();
    }

    @Test
    @DisplayName("recovery re-arms the series so the next breach fires again")
    void clearReArms() {
        given(kafka.send(anyString(), anyString(), anyString())).willReturn(delivered());

        publisher.publishIfNew("s-00042", "pipe_pressure", BREACH);
        publisher.clear("s-00042", "pipe_pressure");

        assertThat(publisher.publishIfNew("s-00042", "pipe_pressure", BREACH)).isTrue();
        verify(kafka, times(2)).send(anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("a delivery failure disarms the series so the next poll retries the alert")
    void failedSendReArms() {
        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        given(kafka.send(anyString(), anyString(), anyString())).willReturn(failed, delivered());

        assertThat(publisher.publishIfNew("s-00042", "pipe_pressure", BREACH)).isTrue();
        // Still armed while the send is in flight ...
        assertThat(publisher.publishIfNew("s-00042", "pipe_pressure", BREACH)).isFalse();

        failed.completeExceptionally(new RuntimeException("broker unreachable"));

        // ... and disarmed the moment the broker reports the loss.
        assertThat(publisher.publishIfNew("s-00042", "pipe_pressure", BREACH)).isTrue();
        verify(kafka, times(2)).send(anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("a serialisation failure disarms the series and propagates")
    void serialisationFailureReArms() {
        ObjectMapper broken = mock(ObjectMapper.class);
        given(broken.writeValueAsString(any())).willThrow(new IllegalStateException("cannot serialise"));
        AlertPublisher withBrokenMapper = new AlertPublisher(kafka, broken);
        given(kafka.send(anyString(), anyString(), anyString())).willReturn(delivered());

        assertThatThrownBy(() -> withBrokenMapper.publishIfNew("s-00042", "pipe_pressure", BREACH))
                .isInstanceOf(IllegalStateException.class);
        verify(kafka, times(0)).send(anyString(), anyString(), anyString());

        // Key released: a healthy mapper (simulated by the same publisher's next attempt succeeding)
        // is not what we can swap in, so assert the re-arm through the failure repeating instead of
        // being swallowed as a "duplicate".
        assertThatThrownBy(() -> withBrokenMapper.publishIfNew("s-00042", "pipe_pressure", BREACH))
                .isInstanceOf(IllegalStateException.class);
    }

    private static CompletableFuture<SendResult<String, String>> delivered() {
        return CompletableFuture.completedFuture(null);
    }
}
