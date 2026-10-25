package inno.ipr.topology_service.graphql;

import inno.ipr.topology_service.config.GraphQlConfig;
import inno.ipr.topology_service.graphql.dto.CrewDto;
import inno.ipr.topology_service.graphql.dto.ImpactSummaryDto;
import inno.ipr.topology_service.graphql.dto.ImpactSummaryDto.LabelCountDto;
import inno.ipr.topology_service.graphql.dto.PathDto;
import inno.ipr.topology_service.graphql.dto.VertexDto;
import inno.ipr.topology_service.service.TopologyQueryService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.graphql.test.autoconfigure.GraphQlTest;
import org.springframework.context.annotation.Import;
import org.springframework.graphql.test.tester.GraphQlTester;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.List;
import java.util.Map;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

/** The GraphQL contract of the aggregation / path / hybrid queries over a mocked service (JSON scalar from {@link GraphQlConfig}). */
@GraphQlTest(TopologyGraphQLController.class)
@Import(GraphQlConfig.class)
class TopologyGraphQLControllerTest {

    @Autowired
    private GraphQlTester graphQl;

    @MockitoBean
    private TopologyQueryService service;

    @Test
    @DisplayName("impactSummary maps totals and per-label counts")
    void impactSummary() {
        given(service.impactSummary("node-5")).willReturn(new ImpactSummaryDto("node-5", 2, 204,
                List.of(new LabelCountDto("ResidentialBuilding", 2))));

        graphQl.document("{ impactSummary(id: \"node-5\") { sourceId totalObjects apartments byLabel { label count } } }")
                .execute()
                .path("impactSummary.apartments").entity(Integer.class).isEqualTo(204)
                .path("impactSummary.totalObjects").entity(Integer.class).isEqualTo(2)
                .path("impactSummary.byLabel[0].label").entity(String.class).isEqualTo("ResidentialBuilding");
    }

    @Test
    @DisplayName("paths uses the schema default limit and returns vertices in order")
    void pathsDefaultLimit() {
        given(service.findPaths("sub-1", "bld-12", 5)).willReturn(List.of(new PathDto(1, List.of(
                new VertexDto("sub-1", "Substation", Map.of("id", "sub-1")),
                new VertexDto("bld-12", "ResidentialBuilding", Map.of("id", "bld-12", "apartments", 84))))));

        graphQl.document("{ paths(from: \"sub-1\", to: \"bld-12\") { length nodes { id label properties } } }")
                .execute()
                .path("paths[0].length").entity(Integer.class).isEqualTo(1)
                .path("paths[0].nodes[*].id").entityList(String.class).containsExactly("sub-1", "bld-12")
                .path("paths[0].nodes[1].properties.apartments").entity(Integer.class).isEqualTo(84);
        verify(service).findPaths("sub-1", "bld-12", 5);
    }

    @Test
    @DisplayName("responsibleCrews maps the joined relational rows")
    void responsibleCrews() {
        given(service.findResponsibleCrews("node-5")).willReturn(List.of(
                new CrewDto("crew-north", "North district crew", null, List.of("bld-12", "bld-19"))));

        graphQl.document("{ responsibleCrews(id: \"node-5\") { crewId name phone assets } }")
                .execute()
                .path("responsibleCrews[0].crewId").entity(String.class).isEqualTo("crew-north")
                .path("responsibleCrews[0].phone").valueIsNull()
                .path("responsibleCrews[0].assets").entityList(String.class).containsExactly("bld-12", "bld-19");
    }
}
