package inno.ipr.topology_service.age;

import inno.ipr.topology_service.exception.AgeQueryException;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;

/**
 * Turns the raw agtype text that AGE returns from a {@code cypher(...)} SELECT into plain Java.
 *
 * <p>agtype is a JSON superset, so scalars come back quoted (e.g. {@code "sub-1"}) and property
 * bags come back as JSON objects (e.g. {@code {"name": "Central Substation", "voltage_kv": 110}}).
 * Reading them with the shared Jackson {@link ObjectMapper} unwraps string escapes and coerces
 * numbers/booleans to their natural Java types — replacing the ad-hoc quote-stripping that used to
 * live in {@code TopologyQueryService}.
 *
 * <p>Only the JSON-compatible subset of agtype is handled (strings, numbers, booleans, objects).
 * The seed graph uses nothing else; extended agtype types (typed numerics, vertices/edges rendered
 * inline) are out of scope for the read API, which selects {@code n.id}/{@code label(n)}/
 * {@code properties(n)} individually.
 */
@Component
public class AgtypeParser {

    private final ObjectMapper objectMapper;

    public AgtypeParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Unwraps a scalar agtype value to its string form: a quoted agtype string ({@code "sub-1"})
     * is unescaped to {@code sub-1}; anything else (an already-bare token) is returned trimmed.
     * Returns {@code null} for a {@code null} input.
     */
    public String parseScalar(String raw) {
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        if (trimmed.length() >= 2 && trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
            try {
                return objectMapper.readValue(trimmed, String.class);
            } catch (RuntimeException e) {
                throw new AgeQueryException("Failed to parse agtype scalar: " + raw, e);
            }
        }
        return trimmed;
    }

    /**
     * Parses an agtype map (a {@code properties(n)} result) into a {@code Map<String, Object>}.
     * {@code null} and the empty object {@code {}} both yield an empty map.
     */
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
            throw new AgeQueryException("Failed to parse agtype map: " + raw, e);
        }
    }
}
