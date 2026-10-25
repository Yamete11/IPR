package inno.ipr.anomaly_detector_service.detection;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static inno.ipr.anomaly_detector_service.detection.ThresholdRule.Aggregate.MAX;
import static inno.ipr.anomaly_detector_service.detection.ThresholdRule.Aggregate.MIN;
import static inno.ipr.anomaly_detector_service.detection.ThresholdRule.Comparison.ABOVE;
import static inno.ipr.anomaly_detector_service.detection.ThresholdRule.Comparison.BELOW;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Startup validation of the externalised rule set. A silently empty or ambiguous configuration is
 * worse than a crash: the detector would run and never alert.
 */
class ThresholdRuleRegistryTest {

    @Test
    @DisplayName("exposes each configured rule by metric type")
    void buildsRulesFromProperties() {
        ThresholdRuleRegistry registry = new ThresholdRuleRegistry(new AnomalyRulesProperties(List.of(
                new ThresholdRule("pipe_pressure", MIN, BELOW, 0.5),
                new ThresholdRule("cable_temperature", MAX, ABOVE, 120))));

        assertThat(registry.forMetric("pipe_pressure"))
                .hasValueSatisfying(r -> assertThat(r.label()).isEqualTo("pipe_pressure_min<0.5"));
        assertThat(registry.forMetric("cable_temperature"))
                .hasValueSatisfying(r -> assertThat(r.label()).isEqualTo("cable_temperature_max>120"));
        assertThat(registry.forMetric("humidity")).isEmpty();
    }

    @Test
    @DisplayName("an empty or missing rule list fails startup")
    void rejectsEmptyRules() {
        assertThatThrownBy(() -> new ThresholdRuleRegistry(new AnomalyRulesProperties(List.of())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("anomaly.rules");
        assertThatThrownBy(() -> new ThresholdRuleRegistry(new AnomalyRulesProperties(null)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("the same metric configured twice fails startup rather than picking one silently")
    void rejectsDuplicateMetric() {
        assertThatThrownBy(() -> new ThresholdRuleRegistry(new AnomalyRulesProperties(List.of(
                new ThresholdRule("pipe_pressure", MIN, BELOW, 0.5),
                new ThresholdRule("pipe_pressure", MAX, ABOVE, 9)))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("pipe_pressure");
    }
}
