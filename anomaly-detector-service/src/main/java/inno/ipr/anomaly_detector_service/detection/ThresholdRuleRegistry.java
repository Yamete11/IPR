package inno.ipr.anomaly_detector_service.detection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The rule set from {@link AnomalyRulesProperties}, indexed by metric. Rejects an empty list and a
 * metric configured twice at startup, so the detector never runs silently with no rules or with an
 * ambiguous one.
 *
 * <p>{@link #forMetric(String)} returns the rule monitoring a metric, or empty when the metric is not
 * monitored.
 */
@Component
public class ThresholdRuleRegistry {

    private static final Logger log = LoggerFactory.getLogger(ThresholdRuleRegistry.class);

    private final Map<String, ThresholdRule> byMetric = new HashMap<>();

    public ThresholdRuleRegistry(AnomalyRulesProperties properties) {
        List<ThresholdRule> configured = properties.rules();
        if (configured == null || configured.isEmpty()) {
            throw new IllegalStateException("anomaly.rules must list at least one threshold rule");
        }
        for (ThresholdRule rule : configured) {
            if (byMetric.putIfAbsent(rule.metricType(), rule) != null) {
                throw new IllegalStateException(
                        "anomaly.rules lists metric '" + rule.metricType() + "' more than once");
            }
            log.info("Threshold rule loaded: {}", rule.label());
        }
    }

    public Optional<ThresholdRule> forMetric(String metricType) {
        return Optional.ofNullable(byMetric.get(metricType));
    }
}
