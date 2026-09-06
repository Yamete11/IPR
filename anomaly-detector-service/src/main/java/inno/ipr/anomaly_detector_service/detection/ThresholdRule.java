package inno.ipr.anomaly_detector_service.detection;

import inno.ipr.anomaly_detector_service.domain.MetricWindow;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.ToDoubleFunction;

/**
 * A per-{@code metric_type} threshold rule evaluated against a {@link MetricWindow}. Rules read the
 * boundary aggregate ({@code min} or {@code max}) rather than the average, so any excursion within
 * the 5-minute bucket trips the rule even when it is surrounded by healthy readings.
 *
 * <p>The active rule set is configured in {@code application.yaml} under {@code anomaly.rules[]}
 * (bound by {@link AnomalyRulesProperties}) and validated once at startup by
 * {@link ThresholdRuleRegistry}. Build instances with {@link #of}, which derives the {@code label}
 * so the wire format is uniform: {@code <metric>_<min|max><op><threshold>}, e.g.
 * {@code pipe_pressure_min<0.5}.
 *
 * @param metricType metric this rule applies to, e.g. {@code pipe_pressure}
 * @param aggregate  which boundary of the window to test
 * @param comparison breach direction against {@code threshold}
 * @param threshold  the limit
 * @param label      human-readable rule id carried on the alert, e.g. {@code pipe_pressure_min<0.5}
 */
public record ThresholdRule(
        String metricType,
        Aggregate aggregate,
        Comparison comparison,
        double threshold,
        String label) {

    /** Which boundary value of the window a rule reads. */
    public enum Aggregate {
        MIN(MetricWindow::min, "min"),
        MAX(MetricWindow::max, "max");

        private final ToDoubleFunction<MetricWindow> extractor;
        private final String token;

        Aggregate(ToDoubleFunction<MetricWindow> extractor, String token) {
            this.extractor = extractor;
            this.token = token;
        }

        double of(MetricWindow window) {
            return extractor.applyAsDouble(window);
        }
    }

    /** Breach direction: the value is out of bounds when it falls below/above the threshold. */
    public enum Comparison {
        BELOW("<") {
            @Override
            boolean breached(double value, double threshold) {
                return value < threshold;
            }
        },
        ABOVE(">") {
            @Override
            boolean breached(double value, double threshold) {
                return value > threshold;
            }
        };

        private final String symbol;

        Comparison(String symbol) {
            this.symbol = symbol;
        }

        abstract boolean breached(double value, double threshold);
    }

    /**
     * Builds a rule with a derived label. The threshold is rendered without a spurious {@code .0}
     * ({@code 120.0} → {@code 120}) but otherwise verbatim ({@code 0.5} stays {@code 0.5}).
     */
    public static ThresholdRule of(String metricType, Aggregate aggregate, Comparison comparison,
                                   double threshold) {
        if (metricType == null || metricType.isBlank()) {
            throw new IllegalArgumentException("metricType must not be blank");
        }
        if (aggregate == null || comparison == null) {
            throw new IllegalArgumentException("aggregate and comparison are required for " + metricType);
        }
        if (!Double.isFinite(threshold)) {
            throw new IllegalArgumentException("threshold must be finite for " + metricType + ": " + threshold);
        }
        String label = metricType + "_" + aggregate.token + comparison.symbol + formatThreshold(threshold);
        return new ThresholdRule(metricType, aggregate, comparison, threshold, label);
    }

    private static String formatThreshold(double threshold) {
        return BigDecimal.valueOf(threshold).stripTrailingZeros().toPlainString();
    }

    /**
     * Evaluates this rule against a window, returning the breaching value and rule label when the
     * threshold is crossed, otherwise empty.
     */
    public Optional<Breach> evaluate(MetricWindow window) {
        double value = aggregate.of(window);
        if (comparison.breached(value, threshold)) {
            return Optional.of(new Breach(value, label));
        }
        return Optional.empty();
    }

    /**
     * A tripped rule: the boundary value that crossed the threshold and the rule label that fired.
     */
    public record Breach(double value, String rule) {
    }
}
