package inno.ipr.anomaly_detector_service.alert;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * A critical event published to the {@code infrastructure-alerts} Kafka topic, keyed by
 * {@code sensor_id}. The wire format is snake_case per the design contract in
 * {@code docs/microservices.md}; {@link JsonProperty} binds those names to the record's camelCase
 * components. Consumed downstream by the {@code ai-incident-responder}.
 *
 * <p>Example: {@code {"alert_id":"a-1001","sensor_id":"s-00042","metric_type":"pipe_pressure",
 * "value":0.0,"detected_at":"2026-07-25T12:00:00Z","rule":"pipe_pressure_min<0.5"}}
 *
 * @param alertId    unique alert id, e.g. {@code a-1001}
 * @param sensorId   sensor whose reading tripped the rule
 * @param metricType the breached metric
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
