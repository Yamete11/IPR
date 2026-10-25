package inno.ipr.topology_service.graphql;

import inno.ipr.topology_service.graphql.dto.CrewDto;
import inno.ipr.topology_service.graphql.dto.EdgeDto;
import inno.ipr.topology_service.graphql.dto.ImpactSummaryDto;
import inno.ipr.topology_service.graphql.dto.PathDto;
import inno.ipr.topology_service.graphql.dto.VertexDto;
import inno.ipr.topology_service.service.TopologyQueryService;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

import java.util.List;

/**
 * GraphQL read API over the {@code city_topology} graph. Method names match the {@code Query}
 * fields in {@code schema.graphqls}; each delegates to {@link TopologyQueryService}.
 */
@Controller
public class TopologyGraphQLController {

    private final TopologyQueryService queryService;

    public TopologyGraphQLController(TopologyQueryService queryService) {
        this.queryService = queryService;
    }

    @QueryMapping
    public List<VertexDto> vertices() {
        return queryService.findAllVertices();
    }

    @QueryMapping
    public List<EdgeDto> edges() {
        return queryService.findAllEdges();
    }

    @QueryMapping
    public List<VertexDto> dependents(@Argument String id) {
        return queryService.findDependents(id);
    }

    @QueryMapping
    public List<VertexDto> dependencies(@Argument String id) {
        return queryService.findDependencies(id);
    }

    @QueryMapping
    public ImpactSummaryDto impactSummary(@Argument String id) {
        return queryService.impactSummary(id);
    }

    @QueryMapping
    public List<PathDto> paths(@Argument String from, @Argument String to, @Argument int limit) {
        return queryService.findPaths(from, to, limit);
    }

    @QueryMapping
    public List<CrewDto> responsibleCrews(@Argument String id) {
        return queryService.findResponsibleCrews(id);
    }
}
