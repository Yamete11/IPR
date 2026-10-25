package inno.ipr.ingestion_service.exception;

/**
 * Thrown when a {@code city-telemetry} record cannot be turned into a valid
 * {@link inno.ipr.ingestion_service.telemetry.TelemetryMessage} — malformed JSON or a failed
 * field-level check. The raw payload is not carried here: the error handler logs it straight from
 * the Kafka record before routing the record to {@code city-telemetry.DLQ}.
 *
 * <p>Unchecked on purpose: it propagates out of the {@code @KafkaListener}, where the container's
 * {@code DefaultErrorHandler} + {@code DeadLetterPublishingRecoverer} take over (see
 * {@code KafkaConfig}). Retrying a poison message never helps, so {@code KafkaConfig} registers it
 * with {@code addNotRetryableExceptions} and it dead-letters on the first failure.
 */
public class InvalidTelemetryException extends RuntimeException {

    public InvalidTelemetryException(String message) {
        super(message);
    }

    public InvalidTelemetryException(String message, Throwable cause) {
        super(message, cause);
    }
}
