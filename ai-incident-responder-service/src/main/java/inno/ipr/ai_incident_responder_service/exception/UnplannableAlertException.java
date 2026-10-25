package inno.ipr.ai_incident_responder_service.exception;

/**
 * Thrown when Claude answered but the answer cannot become an incident plan — a safety refusal, a
 * response truncated at {@code max_tokens}, or no structured content at all. The same prompt would
 * fail the same way on every attempt, so the Kafka error handler treats this as
 * <em>non-retryable</em> and dead-letters the alert instead of re-billing a doomed call at the head
 * of its partition forever. The circuit breaker ignores it too: the API is up, the alert is the
 * problem.
 */
public class UnplannableAlertException extends RuntimeException {

    public UnplannableAlertException(String message) {
        super(message);
    }
}
