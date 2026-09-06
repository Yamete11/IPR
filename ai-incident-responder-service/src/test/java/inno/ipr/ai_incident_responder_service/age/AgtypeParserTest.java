package inno.ipr.ai_incident_responder_service.age;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** agtype text → Java, as AGE actually returns it from a {@code cypher()} SELECT. */
class AgtypeParserTest {

    private final AgtypeParser parser = new AgtypeParser(JsonMapper.builder().build());

    @Test
    @DisplayName("a quoted scalar is unwrapped, with JSON escapes decoded")
    void unwrapsQuotedScalar() {
        assertThat(parser.parseScalar("\"node-5\"")).isEqualTo("node-5");
        assertThat(parser.parseScalar("  \"DistributionNode\"  ")).isEqualTo("DistributionNode");
        assertThat(parser.parseScalar("\"North \\\"Main\\\" Node\"")).isEqualTo("North \"Main\" Node");
    }

    @Test
    @DisplayName("an unquoted token and null pass through")
    void bareTokens() {
        assertThat(parser.parseScalar("42")).isEqualTo("42");
        assertThat(parser.parseScalar(" true ")).isEqualTo("true");
        assertThat(parser.parseScalar(null)).isNull();
    }

    @Test
    @DisplayName("a property map is parsed with numbers and booleans coerced to natural types")
    void parsesMap() {
        Map<String, Object> props = parser.parseMap("{\"name\": \"Central Substation\", \"voltage_kv\": 110, \"live\": true}");

        assertThat(props)
                .containsEntry("name", "Central Substation")
                .containsEntry("voltage_kv", 110)
                .containsEntry("live", true);
    }

    @Test
    @DisplayName("null, blank, 'null' and '{}' are all the empty map")
    void emptyMaps() {
        assertThat(parser.parseMap(null)).isEmpty();
        assertThat(parser.parseMap("   ")).isEmpty();
        assertThat(parser.parseMap("null")).isEmpty();
        assertThat(parser.parseMap("{}")).isEmpty();
    }

    @Test
    @DisplayName("malformed agtype is a loud failure naming the input")
    void malformedIsLoud() {
        assertThatThrownBy(() -> parser.parseMap("{not json"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("{not json");
        // Only a value wrapped in quotes on both ends is treated as a JSON string; anything else is
        // passed through verbatim rather than guessed at.
        assertThat(parser.parseScalar("\"unterminated")).isEqualTo("\"unterminated");
    }
}
