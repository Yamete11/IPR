package inno.ipr.ingestion_service.exception;

/**
 * Thrown when a {@code city-telemetry} record cannot be turned into a valid
 * {@link inno.ipr.ingestion_service.telemetry.TelemetryMessage} — malformed JSON or a failed
 * field-level check. The raw payload is retained so the error handler can log it before the record
 * is routed to the dead-letter topic ({@code city-telemetry.DLQ}); {@code KafkaConfig}'s recoverer
 * reads it back out of the cause chain via {@code payload()}.
 *
 * <p>Unchecked on purpose: it propagates out of the {@code @KafkaListener}, where the container's
 * {@code DefaultErrorHandler} + {@code DeadLetterPublishingRecoverer} take over (see
 * {@code KafkaConfig}). Retrying a poison message never helps, so {@code KafkaConfig} registers it
 * with {@code addNotRetryableExceptions} and it dead-letters on the first failure.
 */
public class InvalidTelemetryException extends RuntimeException {

    private final String payload;

    public InvalidTelemetryException(String message, String payload) {
        super(message);
        this.payload = payload;
    }

    public InvalidTelemetryException(String message, String payload, Throwable cause) {
        super(message, cause);
        this.payload = payload;
    }

    public String payload() {
        return payload;
    }
}
