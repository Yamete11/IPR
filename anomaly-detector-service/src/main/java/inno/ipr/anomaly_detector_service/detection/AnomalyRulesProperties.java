package inno.ipr.anomaly_detector_service.detection;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Binding target for {@code anomaly.rules[]} in {@code application.yaml}: each entry binds directly
 * to a {@link ThresholdRule} ({@code metric-type}, {@code aggregate}, {@code comparison},
 * {@code threshold}), so an invalid entry fails binding with its index. {@link ThresholdRuleRegistry}
 * checks the list as a whole.
 *
 * @param rules one entry per monitored metric
 */
@ConfigurationProperties(prefix = "anomaly")
public record AnomalyRulesProperties(List<ThresholdRule> rules) {
}
