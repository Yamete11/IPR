package inno.ipr.topology_service.graphql;

import inno.ipr.topology_service.graphql.dto.EdgeDto;
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
}
