package inno.ipr.ingestion_service.exception;

/**
 * Thrown by {@code TelemetryWriter} when the database refuses one reading of a batch for good (a
 * {@code DataIntegrityViolationException} on its own single-row insert). {@link #index()} is that
 * reading's position in the list handed to the writer.
 *
 * <p>It carries no Kafka types on purpose: the writer only knows about readings and the database.
 * {@code TelemetryConsumer} translates it into Kafka's {@code BatchListenerFailedException}, keeping
 * the original {@code DataIntegrityViolationException} as the cause, which is what
 * {@code KafkaConfig} classifies as non-retryable and dead-letters.
 */
public class RejectedReadingException extends RuntimeException {

    private final int index;

    public RejectedReadingException(int index, RuntimeException cause) {
        super("Database rejected the reading at batch index " + index + ": " + cause.getMessage(), cause);
        this.index = index;
    }

    public int index() {
        return index;
    }
}
