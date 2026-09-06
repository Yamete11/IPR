package inno.ipr.anomaly_detector_service.detection;

import inno.ipr.anomaly_detector_service.alert.AlertPublisher;
import inno.ipr.anomaly_detector_service.domain.MetricWindow;
import inno.ipr.anomaly_detector_service.repository.MetricAggregateRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * The detection loop. On a fixed schedule it pulls each series' latest 5-minute window from
 * {@link MetricAggregateRepository}, applies the matching {@link ThresholdRule}, and drives the
 * {@link AlertPublisher} edge-trigger: a breach calls {@code publishIfNew}, a healthy window calls
 * {@code clear}. This service polls TimescaleDB — it does not consume Kafka — so detection scales
 * independently of ingestion write throughput.
 */
@Component
public class AnomalyScheduler {

    private static final Logger log = LoggerFactory.getLogger(AnomalyScheduler.class);

    private final MetricAggregateRepository repository;
    private final AlertPublisher publisher;
    private final ThresholdRuleRegistry rules;
    private final Duration lookback;

    public AnomalyScheduler(MetricAggregateRepository repository,
                            AlertPublisher publisher,
                            ThresholdRuleRegistry rules,
                            @Value("${anomaly.lookback:15m}") Duration lookback) {
        this.repository = repository;
        this.publisher = publisher;
        this.rules = rules;
        this.lookback = lookback;
    }

    // initialDelay defers the first poll one interval so the ContinuousAggregateInitializer
    // (an ApplicationRunner, which Spring may run after scheduled tasks start) has created
    // metrics_5m_avg before the loop first queries it.
    @Scheduled(fixedDelayString = "${anomaly.poll-interval-ms:15000}",
            initialDelayString = "${anomaly.poll-interval-ms:15000}")
    public void poll() {
        List<MetricWindow> windows = repository.findLatestWindows(lookback);
        log.debug("Polled {} metric windows (lookback {})", windows.size(), lookback);

        for (MetricWindow window : windows) {
            Optional<ThresholdRule> rule = rules.forMetric(window.metricType());
            if (rule.isEmpty()) {
                continue; // no rule monitors this metric
            }
            Optional<ThresholdRule.Breach> breach = rule.get().evaluate(window);
            if (breach.isPresent()) {
                publisher.publishIfNew(window.sensorId(), window.metricType(), breach.get());
            } else {
                publisher.clear(window.sensorId(), window.metricType());
            }
        }
    }
}
