package inno.ipr.topology_service.exception;

/**
 * Wraps failures that happen while talking to the Apache AGE graph (Cypher execution,
 * graph bootstrap) so callers get an unchecked, domain-specific exception instead of a
 * raw {@code DataAccessException}/{@code SQLException}.
 */
public class AgeQueryException extends RuntimeException {

    public AgeQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public AgeQueryException(String message) {
        super(message);
    }
}
