package inno.ipr.anomaly_detector_service.detection;

import inno.ipr.anomaly_detector_service.alert.AlertPublisher;
import inno.ipr.anomaly_detector_service.detection.AnomalyRulesProperties.Rule;
import inno.ipr.anomaly_detector_service.domain.MetricWindow;
import inno.ipr.anomaly_detector_service.repository.MetricAggregateRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static inno.ipr.anomaly_detector_service.detection.ThresholdRule.Aggregate.MAX;
import static inno.ipr.anomaly_detector_service.detection.ThresholdRule.Aggregate.MIN;
import static inno.ipr.anomaly_detector_service.detection.ThresholdRule.Comparison.ABOVE;
import static inno.ipr.anomaly_detector_service.detection.ThresholdRule.Comparison.BELOW;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/** One poll: each window is routed to its rule, and the outcome drives the publisher's edge trigger. */
class AnomalySchedulerTest {

    private final MetricAggregateRepository repository = mock(MetricAggregateRepository.class);
    private final AlertPublisher publisher = mock(AlertPublisher.class);
    private final ThresholdRuleRegistry rules = new ThresholdRuleRegistry(new AnomalyRulesProperties(List.of(
            new Rule("pipe_pressure", MIN, BELOW, 0.5),
            new Rule("cable_temperature", MAX, ABOVE, 120))));
    private final AnomalyScheduler scheduler = new AnomalyScheduler(repository, publisher, rules, Duration.ofMinutes(15));

    private static MetricWindow window(String sensor, String metric, double min, double max) {
        return new MetricWindow(sensor, metric, Instant.EPOCH, (min + max) / 2, min, max, 5);
    }

    @Test
    @DisplayName("a breaching window publishes with the rule's value and label")
    void breachPublishes() {
        given(repository.findLatestWindows(any())).willReturn(List.of(window("s-00042", "pipe_pressure", 0.0, 3.9)));

        scheduler.poll();

        ArgumentCaptor<ThresholdRule.Breach> breach = ArgumentCaptor.forClass(ThresholdRule.Breach.class);
        verify(publisher).publishIfNew(eq("s-00042"), eq("pipe_pressure"), breach.capture());
        assertThat(breach.getValue().value()).isZero();
        assertThat(breach.getValue().rule()).isEqualTo("pipe_pressure_min<0.5");
        verify(publisher, never()).clear(anyString(), anyString());
    }

    @Test
    @DisplayName("a healthy window clears the series")
    void healthyClears() {
        given(repository.findLatestWindows(any())).willReturn(List.of(window("s-00042", "pipe_pressure", 3.5, 4.5)));

        scheduler.poll();

        verify(publisher).clear("s-00042", "pipe_pressure");
        verify(publisher, never()).publishIfNew(anyString(), anyString(), any());
    }

    @Test
    @DisplayName("a metric with no rule is ignored entirely")
    void unmonitoredMetricIsSkipped() {
        given(repository.findLatestWindows(any())).willReturn(List.of(window("s-00999", "humidity", 0.0, 0.0)));

        scheduler.poll();

        verifyNoInteractions(publisher);
    }

    @Test
    @DisplayName("the lookback is passed through to the repository")
    void passesLookback() {
        given(repository.findLatestWindows(any())).willReturn(List.of());

        scheduler.poll();

        verify(repository).findLatestWindows(Duration.ofMinutes(15));
    }
}
