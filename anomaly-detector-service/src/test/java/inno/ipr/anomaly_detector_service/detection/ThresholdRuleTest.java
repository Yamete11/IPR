package inno.ipr.anomaly_detector_service.detection;

import inno.ipr.anomaly_detector_service.domain.MetricWindow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;

import static inno.ipr.anomaly_detector_service.detection.ThresholdRule.Aggregate.MAX;
import static inno.ipr.anomaly_detector_service.detection.ThresholdRule.Aggregate.MIN;
import static inno.ipr.anomaly_detector_service.detection.ThresholdRule.Comparison.ABOVE;
import static inno.ipr.anomaly_detector_service.detection.ThresholdRule.Comparison.BELOW;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The rule semantics the alert contract depends on: which boundary of the window is read, on which
 * side of the threshold a breach lies, and the exact {@code rule} label that ends up on the wire.
 *
 * <p>{@code minBelowReadsTheMinimum}: the window spans 0.0–4.0, healthy on average; only the minimum
 * reveals the fault.
 */
class ThresholdRuleTest {

    private static MetricWindow window(double min, double max) {
        return new MetricWindow("s-00042", "pipe_pressure", Instant.EPOCH, min, max);
    }

    @Test
    @DisplayName("a MIN/BELOW rule trips on the window minimum")
    void minBelowReadsTheMinimum() {
        ThresholdRule rule = new ThresholdRule("pipe_pressure", MIN, BELOW, 0.5);

        assertThat(rule.evaluate(window(0.0, 4.0)))
                .hasValueSatisfying(b -> {
                    assertThat(b.value()).isZero();
                    assertThat(b.rule()).isEqualTo("pipe_pressure_min<0.5");
                });
    }

    @Test
    @DisplayName("a MAX/ABOVE rule trips on the window maximum")
    void maxAboveReadsTheMaximum() {
        ThresholdRule rule = new ThresholdRule("cable_temperature", MAX, ABOVE, 120);

        assertThat(rule.evaluate(window(40.0, 130.0))).hasValueSatisfying(b -> assertThat(b.value()).isEqualTo(130.0));
        assertThat(rule.evaluate(window(40.0, 90.0))).isEmpty();
    }

    @Test
    @DisplayName("sitting exactly on the threshold is not a breach")
    void thresholdItselfIsNotABreach() {
        assertThat(new ThresholdRule("pipe_pressure", MIN, BELOW, 0.5).evaluate(window(0.5, 4.0))).isEmpty();
        assertThat(new ThresholdRule("cable_temperature", MAX, ABOVE, 120).evaluate(window(40.0, 120.0))).isEmpty();
    }

    @ParameterizedTest(name = "{0} {1} {2} {3} -> {4}")
    @CsvSource({
            "pipe_pressure,      MIN, BELOW, 0.5,   pipe_pressure_min<0.5",
            "cable_temperature,  MAX, ABOVE, 120.0, cable_temperature_max>120",
            "substation_voltage, MIN, BELOW, 50,    substation_voltage_min<50",
            "flow_rate,          MAX, ABOVE, 12.75, flow_rate_max>12.75",
    })
    @DisplayName("the label is <metric>_<min|max><op><threshold> with no trailing .0")
    void labelFormat(String metric, ThresholdRule.Aggregate aggregate, ThresholdRule.Comparison comparison,
                     double threshold, String expected) {
        assertThat(new ThresholdRule(metric, aggregate, comparison, threshold).label()).isEqualTo(expected);
    }

    @Test
    @DisplayName("a rule cannot be built from a blank metric, a missing side, or a non-finite threshold")
    void rejectsInvalidDefinitions() {
        assertThatThrownBy(() -> new ThresholdRule(" ", MIN, BELOW, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ThresholdRule("x", null, BELOW, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ThresholdRule("x", MIN, null, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ThresholdRule("x", MIN, BELOW, Double.NaN)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ThresholdRule("x", MIN, BELOW, Double.POSITIVE_INFINITY))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
