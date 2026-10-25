package inno.ipr.anomaly_detector_service.detection;

import inno.ipr.anomaly_detector_service.domain.MetricWindow;

import java.math.BigDecimal;
import java.util.Optional;

/**
 * A per-{@code metric_type} threshold rule evaluated against a {@link MetricWindow}. Rules read a
 * boundary value ({@code min} or {@code max}) rather than the average, so any excursion within the
 * bucket trips the rule even when it is surrounded by healthy readings. Instances are bound straight
 * from {@code anomaly.rules[]} (see {@link AnomalyRulesProperties}); the constructor rejects a blank
 * metric, a missing aggregate or comparison, and a non-finite threshold.
 *
 * <p>{@link Aggregate} names which boundary value of the window a rule reads; {@link Comparison} is
 * the breach direction.
 *
 * <p>{@link #evaluate(MetricWindow)} returns a {@link Breach} (the boundary value and the label) when
 * the value is strictly below/above the threshold, otherwise empty; sitting exactly on the threshold
 * is not a breach.
 *
 * <p>{@link #label()} is the rule id carried on the alert, {@code <metric>_<min|max><op><threshold>},
 * e.g. {@code pipe_pressure_min<0.5}; the threshold drops a spurious {@code .0} ({@code 120.0} →
 * {@code 120}).
 *
 * @param metricType metric this rule applies to, e.g. {@code pipe_pressure}
 * @param aggregate  which boundary of the window to test
 * @param comparison breach direction against {@code threshold}
 * @param threshold  the limit
 */
public record ThresholdRule(
        String metricType,
        Aggregate aggregate,
        Comparison comparison,
        double threshold) {

    public enum Aggregate { MIN, MAX }

    public enum Comparison { BELOW, ABOVE }

    public record Breach(double value, String rule) {
    }

    public ThresholdRule {
        if (metricType == null || metricType.isBlank()) {
            throw new IllegalArgumentException("metricType must not be blank");
        }
        if (aggregate == null || comparison == null) {
            throw new IllegalArgumentException("aggregate and comparison are required for " + metricType);
        }
        if (!Double.isFinite(threshold)) {
            throw new IllegalArgumentException("threshold must be finite for " + metricType + ": " + threshold);
        }
    }

    public Optional<Breach> evaluate(MetricWindow window) {
        double value = switch (aggregate) {
            case MIN -> window.min();
            case MAX -> window.max();
        };
        boolean breached = switch (comparison) {
            case BELOW -> value < threshold;
            case ABOVE -> value > threshold;
        };
        return breached ? Optional.of(new Breach(value, label())) : Optional.empty();
    }

    public String label() {
        String boundary = switch (aggregate) {
            case MIN -> "min";
            case MAX -> "max";
        };
        String operator = switch (comparison) {
            case BELOW -> "<";
            case ABOVE -> ">";
        };
        return metricType + "_" + boundary + operator + BigDecimal.valueOf(threshold).stripTrailingZeros().toPlainString();
    }
}
