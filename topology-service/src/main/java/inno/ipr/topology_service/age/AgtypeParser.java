package inno.ipr.topology_service.age;

import inno.ipr.topology_service.exception.AgeQueryException;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Turns the raw agtype text that AGE returns from a {@code cypher(...)} SELECT into plain Java.
 *
 * <p>agtype is a JSON superset, so scalars come back quoted (e.g. {@code "sub-1"}) and property
 * bags come back as JSON objects (e.g. {@code {"name": "Central Substation", "voltage_kv": 110}}).
 * Reading them with the shared Jackson {@link ObjectMapper} unwraps string escapes and coerces
 * numbers/booleans to their natural Java types.
 *
 * <p>Only the JSON-compatible subset of agtype is handled (strings, numbers, booleans, objects).
 * The seed graph uses nothing else. Entities rendered inline (a {@code nodes(p)} list) carry type
 * annotations such as {@code ::vertex}; {@link #parseVertexList} strips those before reading the
 * JSON. Everything else selects {@code n.id}/{@code label(n)}/{@code properties(n)} individually.
 *
 * <p>{@link #parseScalar(String)} unwraps a scalar agtype value to its string form: a quoted agtype
 * string ({@code "sub-1"}) is unescaped to {@code sub-1}; anything else (an already-bare token) is
 * returned trimmed, and a {@code null} input yields {@code null}. {@link #parseMap(String)} parses an
 * agtype map (a {@code properties(n)} result) into a {@code Map<String, Object>}; {@code null} and
 * the empty object {@code {}} both yield an empty map. {@link #parseLong(String)} parses an agtype
 * integer (a {@code count(...)}/{@code sum(...)}/{@code length(p)} result).
 *
 * <p>{@link #parseVertexList(String)} parses a list of vertices as AGE renders {@code nodes(p)}:
 * {@code [{"id": 8444..., "label": "Substation", "properties": {...}}::vertex, ...]}, returning one
 * map per vertex with the keys {@code id} (AGE's internal graphid), {@code label} and
 * {@code properties}.
 *
 * <p>{@code stripTypeAnnotations(String)} removes the agtype annotations ({@code ::vertex},
 * {@code ::edge}, {@code ::path}, {@code ::numeric}) that follow a value, leaving plain JSON. String
 * literals are copied verbatim, so a property value that happens to contain {@code "::vertex"} is
 * left alone.
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
                throw new AgeQueryException("Failed to parse agtype scalar: " + raw, e);
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
            throw new AgeQueryException("Failed to parse agtype map: " + raw, e);
        }
    }

    public long parseLong(String raw) {
        String value = raw == null ? null : parseScalar(stripTypeAnnotations(raw));
        if (value == null || "null".equals(value)) {
            return 0L;
        }
        try {
            return new BigDecimal(value).longValueExact();
        } catch (RuntimeException e) {
            throw new AgeQueryException("Failed to parse agtype integer: " + raw, e);
        }
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> parseVertexList(String raw) {
        if (raw == null || raw.isBlank()) {
            return List.of();
        }
        try {
            return objectMapper.readValue(stripTypeAnnotations(raw), List.class);
        } catch (RuntimeException e) {
            throw new AgeQueryException("Failed to parse agtype vertex list: " + raw, e);
        }
    }

    static String stripTypeAnnotations(String raw) {
        StringBuilder out = new StringBuilder(raw.length());
        boolean inString = false;
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (inString) {
                out.append(c);
                if (c == '\\' && i + 1 < raw.length()) {
                    out.append(raw.charAt(++i));
                } else if (c == '"') {
                    inString = false;
                }
            } else if (c == '"') {
                inString = true;
                out.append(c);
            } else if (c == ':' && i + 1 < raw.length() && raw.charAt(i + 1) == ':') {
                i++;
                while (i + 1 < raw.length() && Character.isLetter(raw.charAt(i + 1))) {
                    i++;
                }
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }
}
