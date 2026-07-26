package inno.ipr.anomaly_detector_service.detection;

import inno.ipr.anomaly_detector_service.domain.MetricWindow;

import java.util.List;
import java.util.Optional;
import java.util.function.ToDoubleFunction;

/**
 * A per-{@code metric_type} threshold rule evaluated against a {@link MetricWindow}. Rules read the
 * boundary aggregate ({@code min} or {@code max}) rather than the average, so any excursion within
 * the 5-minute bucket trips the rule even when it is surrounded by healthy readings.
 *
 * <p>The rule set is defined in code (see {@link #RULES}), tuned to the faults the
 * sensor-simulator injects. Externalising thresholds and per-sensor rules is deferred.
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
        MIN(MetricWindow::min),
        MAX(MetricWindow::max);

        private final ToDoubleFunction<MetricWindow> extractor;

        Aggregate(ToDoubleFunction<MetricWindow> extractor) {
            this.extractor = extractor;
        }

        double of(MetricWindow window) {
            return extractor.applyAsDouble(window);
        }
    }

    /** Breach direction: the value is out of bounds when it falls below/above the threshold. */
    public enum Comparison {
        BELOW {
            @Override
            boolean breached(double value, double threshold) {
                return value < threshold;
            }
        },
        ABOVE {
            @Override
            boolean breached(double value, double threshold) {
                return value > threshold;
            }
        };

        abstract boolean breached(double value, double threshold);
    }

    /**
     * The active rule set, one entry per monitored metric. Thresholds are placed just inside the
     * healthy operating band so a simulator fault reading trips the corresponding boundary.
     */
    public static final List<ThresholdRule> RULES = List.of(
            new ThresholdRule("pipe_pressure", Aggregate.MIN, Comparison.BELOW, 0.5, "pipe_pressure_min<0.5"),
            new ThresholdRule("cable_temperature", Aggregate.MAX, Comparison.ABOVE, 100.0, "cable_temperature_max>100"),
            new ThresholdRule("substation_voltage", Aggregate.MIN, Comparison.BELOW, 50.0, "substation_voltage_min<50"));

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
