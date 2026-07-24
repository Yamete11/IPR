package inno.ipr.ingestion_service.telemetry;

import inno.ipr.ingestion_service.exception.InvalidTelemetryException;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Turns a raw {@code city-telemetry} JSON payload into a validated {@link TelemetryMessage}.
 *
 * <p>The listener consumes the record as a {@code String} (a plain {@code StringDeserializer}), so
 * both failure modes — malformed JSON and a well-formed message with missing/blank fields — surface
 * here as a single {@link InvalidTelemetryException}. That keeps error handling uniform: whatever
 * goes wrong, the container's error handler routes the record to {@code city-telemetry.DLQ}. Boot 4
 * ships Jackson 3, hence {@code tools.jackson.databind.ObjectMapper}.
 */
@Component
public class TelemetryParser {

    private final ObjectMapper objectMapper;

    public TelemetryParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public TelemetryMessage parse(String json) {
        TelemetryMessage message;
        try {
            message = objectMapper.readValue(json, TelemetryMessage.class);
        } catch (RuntimeException e) {
            throw new InvalidTelemetryException("Malformed telemetry JSON", json, e);
        }
        validate(message, json);
        return message;
    }

    private static void validate(TelemetryMessage m, String json) {
        if (m.timestamp() == null) {
            throw new InvalidTelemetryException("Missing 'timestamp'", json);
        }
        if (isBlank(m.sensorId())) {
            throw new InvalidTelemetryException("Missing 'sensor_id'", json);
        }
        if (isBlank(m.metricType())) {
            throw new InvalidTelemetryException("Missing 'metric_type'", json);
        }
        if (isBlank(m.status())) {
            throw new InvalidTelemetryException("Missing 'status'", json);
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
