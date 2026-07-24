package inno.ipr.ingestion_service.telemetry;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * A single telemetry reading from the {@code city-telemetry} Kafka topic. The JSON is snake_case on
 * the wire (keyed by {@code sensor_id}); the {@link JsonProperty} mappings bind those names to the
 * record's camelCase components.
 *
 * @param timestamp  when the reading was taken (ISO-8601 instant, e.g. {@code 2026-07-24T12:00:00Z})
 * @param sensorId   producing sensor, e.g. {@code s-00042}
 * @param metricType the measured quantity, e.g. {@code pipe_pressure}
 * @param value      the measured value
 * @param status     sensor-reported status, e.g. {@code OK}
 */
public record TelemetryMessage(
        @JsonProperty("timestamp") Instant timestamp,
        @JsonProperty("sensor_id") String sensorId,
        @JsonProperty("metric_type") String metricType,
        @JsonProperty("value") double value,
        @JsonProperty("status") String status) {
}
