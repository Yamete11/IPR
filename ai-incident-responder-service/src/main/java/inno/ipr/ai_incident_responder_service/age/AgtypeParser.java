package inno.ipr.ai_incident_responder_service.age;

import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;

/**
 * Turns the raw agtype text AGE returns from a {@code cypher(...)} SELECT into plain Java. Copied
 * from topology-service — agtype is a JSON superset, so scalars come back quoted (e.g.
 * {@code "node-5"}) and property bags as JSON objects. Jackson 3 ({@code tools.jackson.databind})
 * unwraps escapes and coerces numbers/booleans to their natural types.
 *
 * <p>{@link #parseScalar(String)} unwraps a scalar agtype value ({@code "node-5"} →
 * {@code node-5}); {@code null} stays null. {@link #parseMap(String)} parses an agtype map
 * ({@code properties(n)}); {@code null}/{@code {}} yield an empty map.
 */
@Component
public class AgtypeParser {

    private final ObjectMapper objectMapper;

    public AgtypeParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String parseScalar(String raw) {
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        if (trimmed.length() >= 2 && trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
            try {
                return objectMapper.readValue(trimmed, String.class);
            } catch (RuntimeException e) {
                throw new IllegalStateException("Failed to parse agtype scalar: " + raw, e);
            }
        }
        return trimmed;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> parseMap(String raw) {
        if (raw == null) {
            return Map.of();
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty() || "null".equals(trimmed) || "{}".equals(trimmed)) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(trimmed, Map.class);
        } catch (RuntimeException e) {
            throw new IllegalStateException("Failed to parse agtype map: " + raw, e);
        }
    }
}
