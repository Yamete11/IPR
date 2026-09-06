package inno.ipr.anomaly_detector_service.detection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The validated, immutable rule set built from {@link AnomalyRulesProperties} at startup. Rejects a
 * misconfiguration loudly (empty list, duplicate metric, non-finite threshold) so the detector
 * never runs silently with no rules or with an ambiguous one.
 */
@Component
public class ThresholdRuleRegistry {

    private static final Logger log = LoggerFactory.getLogger(ThresholdRuleRegistry.class);

    private final Map<String, ThresholdRule> byMetric;

    public ThresholdRuleRegistry(AnomalyRulesProperties properties) {
        List<AnomalyRulesProperties.Rule> configured = properties.rules();
        if (configured == null || configured.isEmpty()) {
            throw new IllegalStateException("anomaly.rules must list at least one threshold rule");
        }
        Map<String, ThresholdRule> rules = new LinkedHashMap<>();
        for (AnomalyRulesProperties.Rule r : configured) {
            ThresholdRule rule;
            try {
                rule = ThresholdRule.of(r.metricType(), r.aggregate(), r.comparison(), r.threshold());
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("Invalid anomaly.rules entry: " + e.getMessage(), e);
            }
            if (rules.putIfAbsent(rule.metricType(), rule) != null) {
                throw new IllegalStateException(
                        "anomaly.rules lists metric '" + rule.metricType() + "' more than once");
            }
        }
        this.byMetric = Map.copyOf(rules);
        rules.values().forEach(rule -> log.info("Threshold rule loaded: {}", rule.label()));
    }

    /** The rule monitoring {@code metricType}, or empty when the metric is not monitored. */
    public Optional<ThresholdRule> forMetric(String metricType) {
        return Optional.ofNullable(byMetric.get(metricType));
    }

    /** All configured rules, in configuration order. */
    public List<ThresholdRule> all() {
        return List.copyOf(byMetric.values());
    }
}
