package inno.ipr.anomaly_detector_service.detection;

import inno.ipr.anomaly_detector_service.alert.AlertPublisher;
import inno.ipr.anomaly_detector_service.domain.MetricWindow;
import inno.ipr.anomaly_detector_service.domain.SeriesKey;
import inno.ipr.anomaly_detector_service.repository.MetricAggregateRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The detection loop. On a fixed schedule it pulls each series' recent windows from
 * {@link MetricAggregateRepository}, applies the matching {@link ThresholdRule} and drives the
 * {@link AlertPublisher} edge trigger. It polls TimescaleDB rather than consuming Kafka, so detection
 * scales independently of ingestion write throughput.
 *
 * <p>{@link #poll()} starts after one interval so the {@code ContinuousAggregateInitializer} (an
 * {@code ApplicationRunner}, which may run after scheduled tasks start) has created
 * {@code metrics_5m_avg} first. Series whose metric no rule monitors are skipped. Afterwards
 * {@code retainOnly} disarms series that no longer report at all.
 *
 * <p>{@code evaluate} sees a series' two newest buckets. The series is breaching if <em>either</em>
 * bucket breaches (the newest breach is reported) and healthy only when both are, so a fault at the
 * very end of a bucket still fires after the next bucket has begun, and the trigger stays armed until
 * that bucket ages out of the pair.
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

    @Scheduled(fixedDelayString = "${anomaly.poll-interval-ms:15000}",
            initialDelayString = "${anomaly.poll-interval-ms:15000}")
    public void poll() {
        List<MetricWindow> windows = repository.findRecentWindows(lookback);
        log.debug("Polled {} metric windows (lookback {})", windows.size(), lookback);

        Map<SeriesKey, List<MetricWindow>> bySeries = windows.stream().collect(
                Collectors.groupingBy(MetricWindow::series, LinkedHashMap::new, Collectors.toList()));

        bySeries.forEach((series, recent) -> rules.forMetric(series.metricType())
                .ifPresent(rule -> evaluate(series, rule, recent)));
        publisher.retainOnly(bySeries.keySet());
    }

    private void evaluate(SeriesKey series, ThresholdRule rule, List<MetricWindow> recent) {
        Optional<ThresholdRule.Breach> breach = recent.stream()
                .map(rule::evaluate)
                .flatMap(Optional::stream)
                .findFirst();
        if (breach.isPresent()) {
            publisher.publishIfNew(series, breach.get());
        } else {
            publisher.clear(series);
        }
    }
}
