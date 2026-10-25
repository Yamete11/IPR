package inno.ipr.anomaly_detector_service.detection;

import inno.ipr.anomaly_detector_service.alert.AlertPublisher;
import inno.ipr.anomaly_detector_service.domain.MetricWindow;
import inno.ipr.anomaly_detector_service.domain.SeriesKey;
import inno.ipr.anomaly_detector_service.repository.MetricAggregateRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static inno.ipr.anomaly_detector_service.detection.ThresholdRule.Aggregate.MAX;
import static inno.ipr.anomaly_detector_service.detection.ThresholdRule.Aggregate.MIN;
import static inno.ipr.anomaly_detector_service.detection.ThresholdRule.Comparison.ABOVE;
import static inno.ipr.anomaly_detector_service.detection.ThresholdRule.Comparison.BELOW;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/** One poll: each series is routed to its rule, and the outcome drives the publisher's edge trigger. */
class AnomalySchedulerTest {

    private static final SeriesKey PRESSURE_42 = new SeriesKey("s-00042", "pipe_pressure");

    private final MetricAggregateRepository repository = mock(MetricAggregateRepository.class);
    private final AlertPublisher publisher = mock(AlertPublisher.class);
    private final ThresholdRuleRegistry rules = new ThresholdRuleRegistry(new AnomalyRulesProperties(List.of(
            new ThresholdRule("pipe_pressure", MIN, BELOW, 0.5),
            new ThresholdRule("cable_temperature", MAX, ABOVE, 120))));
    private final AnomalyScheduler scheduler = new AnomalyScheduler(repository, publisher, rules, Duration.ofMinutes(15));

    private static MetricWindow window(String sensor, String metric, double min, double max) {
        return window(sensor, metric, min, max, Instant.EPOCH);
    }

    private static MetricWindow window(String sensor, String metric, double min, double max, Instant bucket) {
        return new MetricWindow(sensor, metric, bucket, min, max);
    }

    @Test
    @DisplayName("a breaching window publishes with the rule's value and label")
    void breachPublishes() {
        given(repository.findRecentWindows(any())).willReturn(List.of(window("s-00042", "pipe_pressure", 0.0, 3.9)));

        scheduler.poll();

        ArgumentCaptor<ThresholdRule.Breach> breach = ArgumentCaptor.forClass(ThresholdRule.Breach.class);
        verify(publisher).publishIfNew(eq(PRESSURE_42), breach.capture());
        assertThat(breach.getValue().value()).isZero();
        assertThat(breach.getValue().rule()).isEqualTo("pipe_pressure_min<0.5");
        verify(publisher, never()).clear(any());
    }

    @Test
    @DisplayName("a healthy window clears the series")
    void healthyClears() {
        given(repository.findRecentWindows(any())).willReturn(List.of(window("s-00042", "pipe_pressure", 3.5, 4.5)));

        scheduler.poll();

        verify(publisher).clear(PRESSURE_42);
        verify(publisher, never()).publishIfNew(any(), any());
    }

    @Test
    @DisplayName("a metric with no rule is ignored entirely")
    void unmonitoredMetricIsSkipped() {
        given(repository.findRecentWindows(any())).willReturn(List.of(window("s-00999", "humidity", 0.0, 0.0)));

        scheduler.poll();

        verify(publisher, never()).publishIfNew(any(), any());
        verify(publisher, never()).clear(any());
    }

    @Test
    @DisplayName("a fault in the previous bucket still fires after a healthy bucket has started")
    void previousBucketBreachPublishes() {
        given(repository.findRecentWindows(any())).willReturn(List.of(
                window("s-00042", "pipe_pressure", 3.5, 4.5, Instant.parse("2026-10-25T10:05:00Z")),
                window("s-00042", "pipe_pressure", 0.0, 4.1, Instant.parse("2026-10-25T10:00:00Z"))));

        scheduler.poll();

        ArgumentCaptor<ThresholdRule.Breach> breach = ArgumentCaptor.forClass(ThresholdRule.Breach.class);
        verify(publisher).publishIfNew(eq(PRESSURE_42), breach.capture());
        assertThat(breach.getValue().value()).isZero();
        verify(publisher, never()).clear(any());
    }

    @Test
    @DisplayName("a series clears only when both of its recent buckets are healthy")
    void bothBucketsHealthyClears() {
        given(repository.findRecentWindows(any())).willReturn(List.of(
                window("s-00042", "pipe_pressure", 3.5, 4.5, Instant.parse("2026-10-25T10:05:00Z")),
                window("s-00042", "pipe_pressure", 3.6, 4.4, Instant.parse("2026-10-25T10:00:00Z"))));

        scheduler.poll();

        verify(publisher).clear(PRESSURE_42);
        verify(publisher, never()).publishIfNew(any(), any());
    }

    @Test
    @DisplayName("every poll hands the publisher the series it saw, so silent series are disarmed")
    void retainsSeenSeries() {
        given(repository.findRecentWindows(any())).willReturn(List.of(
                window("s-00042", "pipe_pressure", 3.5, 4.5),
                window("s-00999", "humidity", 0.0, 0.0)));

        scheduler.poll();

        verify(publisher).retainOnly(Set.of(PRESSURE_42, new SeriesKey("s-00999", "humidity")));
    }

    @Test
    @DisplayName("the lookback is passed through to the repository")
    void passesLookback() {
        given(repository.findRecentWindows(any())).willReturn(List.of());

        scheduler.poll();

        verify(repository).findRecentWindows(Duration.ofMinutes(15));
    }
}
