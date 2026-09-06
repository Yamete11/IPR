package inno.ipr.anomaly_detector_service.detection;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Binding target for {@code anomaly.rules[]} in {@code application.yaml} — the externalised
 * threshold rule set. Each entry names a metric, which boundary aggregate to read, the breach
 * direction and the limit; {@link ThresholdRuleRegistry} turns the list into validated
 * {@link ThresholdRule}s. Other {@code anomaly.*} keys (poll interval, lookback, schema) are bound
 * elsewhere and ignored here.
 *
 * @param rules one entry per monitored metric
 */
@ConfigurationProperties(prefix = "anomaly")
public record AnomalyRulesProperties(List<Rule> rules) {

    /**
     * One configured rule.
     *
     * @param metricType metric this rule applies to, e.g. {@code pipe_pressure}
     * @param aggregate  {@code MIN} or {@code MAX}
     * @param comparison {@code BELOW} or {@code ABOVE}
     * @param threshold  the limit
     */
    public record Rule(String metricType,
                       ThresholdRule.Aggregate aggregate,
                       ThresholdRule.Comparison comparison,
                       double threshold) {
    }
}
