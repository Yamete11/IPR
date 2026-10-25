package inno.ipr.ingestion_service.telemetry;

import inno.ipr.ingestion_service.exception.InvalidTelemetryException;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;

/**
 * Turns a raw {@code city-telemetry} JSON payload into a validated {@link TelemetryMessage}.
 *
 * <p>The listener consumes the record as a {@code String} (a plain {@code StringDeserializer}), so
 * both failure modes — malformed JSON and a well-formed message with missing/blank fields — surface
 * here as a single {@link InvalidTelemetryException}. That keeps error handling uniform: whatever
 * goes wrong, the container's error handler routes the record to {@code city-telemetry.DLQ}. Boot 4
 * ships Jackson 3, hence {@code tools.jackson.databind.ObjectMapper}.
 *
 * <p>{@link #parse(String)} catches only {@link JacksonException}: only Jackson's own failures mean
 * "the producer sent us garbage". Jackson 3 exceptions are all unchecked, so catching
 * {@code RuntimeException} would also swallow our own bugs and dead-letter a perfectly good record
 * under a misleading "malformed JSON" label. A Kafka tombstone (a record with no value at all) is
 * rejected before Jackson sees it, since it carries no reading. A payload that is the JSON literal
 * {@code null} binds to
 * no message at all; it is rejected explicitly, because left alone it would fail later with a
 * {@code NullPointerException} that is not classified as bad data and be retried forever instead of
 * dead-lettered.
 *
 * <p>{@code violation} names the first broken field of a parsed message, or returns {@code null}
 * when it is valid; {@link #parse(String)} turns that name into the exception, so the checks stay
 * plain conditions instead of each building its own exception.
 *
 * <p>{@code violation} rejects a missing or non-finite {@code value}: a dropped value must never
 * reach the hypertable as 0.0, because the anomaly detector thresholds on minimums
 * ({@code pipe_pressure < 0.5}, {@code substation_voltage < 50}), so a fabricated zero raises a
 * CRITICAL alert and costs a real Claude call downstream. The same goes for the non-finite doubles
 * JSON can carry in ({@code 1e400} parses as Infinity).
 *
 * <p>It also rejects a {@code timestamp} more than {@code MAX_CLOCK_SKEW} (5 minutes) ahead of this
 * host's clock — a sensor with a fast clock, or epoch milliseconds that Jackson reads as seconds.
 * Such a row would form the series' newest continuous-aggregate bucket for as long as it stays in
 * the future, hiding every real reading from the anomaly detector, and the retention policy would
 * never drop its chunk.
 */
@Component
public class TelemetryParser {

    private static final Duration MAX_CLOCK_SKEW = Duration.ofMinutes(5);

    private final ObjectMapper objectMapper;

    public TelemetryParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public TelemetryMessage parse(String json) {
        if (json == null) {
            throw new InvalidTelemetryException("Null payload (tombstone)");
        }
        TelemetryMessage message;
        try {
            message = objectMapper.readValue(json, TelemetryMessage.class);
        } catch (JacksonException e) {
            throw new InvalidTelemetryException("Malformed telemetry JSON", e);
        }
        if (message == null) {
            throw new InvalidTelemetryException("Empty telemetry payload");
        }
        String violation = violation(message);
        if (violation != null) {
            throw new InvalidTelemetryException(violation);
        }
        return message;
    }

    private static String violation(TelemetryMessage m) {
        if (m.timestamp() == null) {
            return "Missing 'timestamp'";
        }
        if (m.timestamp().isAfter(Instant.now().plus(MAX_CLOCK_SKEW))) {
            return "'timestamp' is in the future: " + m.timestamp();
        }
        if (isBlank(m.sensorId())) {
            return "Missing 'sensor_id'";
        }
        if (isBlank(m.metricType())) {
            return "Missing 'metric_type'";
        }
        if (m.value() == null) {
            return "Missing 'value'";
        }
        if (!Double.isFinite(m.value())) {
            return "Non-finite 'value': " + m.value();
        }
        if (isBlank(m.status())) {
            return "Missing 'status'";
        }
        return null;
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
