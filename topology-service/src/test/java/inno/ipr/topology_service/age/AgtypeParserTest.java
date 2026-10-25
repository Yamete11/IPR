package inno.ipr.topology_service.age;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AgtypeParserTest {

    private final AgtypeParser parser = new AgtypeParser(JsonMapper.builder().build());

    @Test
    void parsesVertexListAsRenderedByNodesOfPath() {
        String raw = "[{\"id\": 844424930131969, \"label\": \"Substation\", \"properties\": {\"id\": \"sub-1\"}}::vertex, "
                + "{\"id\": 1970324836974593, \"label\": \"ResidentialBuilding\", "
                + "\"properties\": {\"id\": \"bld-12\", \"apartments\": 84}}::vertex]";
        List<Map<String, Object>> vertices = parser.parseVertexList(raw);
        assertThat(vertices).hasSize(2);
        assertThat(vertices.get(1)).containsEntry("label", "ResidentialBuilding");
        assertThat(vertices.get(1).get("properties")).isEqualTo(Map.of("id", "bld-12", "apartments", 84));
    }

    @Test
    void leavesAnnotationLookalikesInsideStringsAlone() {
        assertThat(AgtypeParser.stripTypeAnnotations("[{\"name\": \"a::vertex \\\" b\"}::vertex]::path"))
                .isEqualTo("[{\"name\": \"a::vertex \\\" b\"}]");
    }

    @Test
    void parsesAggregateIntegers() {
        assertThat(parser.parseLong("204")).isEqualTo(204L);
        assertThat(parser.parseLong("2::numeric")).isEqualTo(2L);
        assertThat(parser.parseLong(null)).isZero();
    }
}
