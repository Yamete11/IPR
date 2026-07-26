package inno.ipr.ai_incident_responder_service.exception;

/**
 * Thrown when an {@code infrastructure-alerts} record cannot be parsed or is missing required
 * fields — a poison message. The Kafka error handler treats this as <em>non-retryable</em> and
 * routes the record straight to the DLQ (retrying malformed JSON would never succeed), unlike a
 * transient Claude/DB failure which is worth retrying.
 */
public class InvalidAlertException extends RuntimeException {

    public InvalidAlertException(String message, String payload) {
        super(message + " | payload=" + payload);
    }

    public InvalidAlertException(String message, String payload, Throwable cause) {
        super(message + " | payload=" + payload, cause);
    }
}
