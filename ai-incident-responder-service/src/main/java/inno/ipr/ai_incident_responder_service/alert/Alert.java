package inno.ipr.ai_incident_responder_service.alert;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * A critical event consumed from the {@code infrastructure-alerts} Kafka topic (produced by
 * anomaly-detector-service). Snake_case on the wire per the design contract; {@link JsonProperty}
 * binds those names to the record's camelCase components.
 *
 * @param alertId    unique alert id, e.g. {@code a-1001}
 * @param sensorId   sensor whose reading tripped the rule, e.g. {@code s-00042}
 * @param metricType the breached metric, e.g. {@code pipe_pressure}
 * @param value      the boundary value that crossed the threshold
 * @param detectedAt when the detector observed the breach
 * @param rule       the rule label that fired, e.g. {@code pipe_pressure_min<0.5}
 */
public record Alert(
        @JsonProperty("alert_id") String alertId,
        @JsonProperty("sensor_id") String sensorId,
        @JsonProperty("metric_type") String metricType,
        @JsonProperty("value") double value,
        @JsonProperty("detected_at") Instant detectedAt,
        @JsonProperty("rule") String rule) {
}
