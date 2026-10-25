package inno.ipr.topology_service.graphql;

import inno.ipr.topology_service.graphql.dto.CrewDto;
import inno.ipr.topology_service.graphql.dto.EdgeDto;
import inno.ipr.topology_service.graphql.dto.ImpactSummaryDto;
import inno.ipr.topology_service.graphql.dto.PathDto;
import inno.ipr.topology_service.graphql.dto.VertexDto;
import inno.ipr.topology_service.repository.TopologyRepository;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

import java.util.List;

/**
 * GraphQL read API over the {@code city_topology} graph. Method names match the {@code Query}
 * fields in {@code schema.graphqls}; each delegates to {@link TopologyRepository}, which owns the
 * Cypher and the agtype → DTO mapping.
 */
@Controller
public class TopologyGraphQLController {

    private final TopologyRepository repository;

    public TopologyGraphQLController(TopologyRepository repository) {
        this.repository = repository;
    }

    @QueryMapping
    public List<VertexDto> vertices() {
        return repository.findAllVertices();
    }

    @QueryMapping
    public List<EdgeDto> edges() {
        return repository.findAllEdges();
    }

    @QueryMapping
    public List<VertexDto> dependents(@Argument String id) {
        return repository.findDependents(id);
    }

    @QueryMapping
    public List<VertexDto> dependencies(@Argument String id) {
        return repository.findDependencies(id);
    }

    @QueryMapping
    public ImpactSummaryDto impactSummary(@Argument String id) {
        return repository.impactSummary(id);
    }

    @QueryMapping
    public List<PathDto> paths(@Argument String from, @Argument String to, @Argument int limit) {
        return repository.findPaths(from, to, limit);
    }

    @QueryMapping
    public List<CrewDto> responsibleCrews(@Argument String id) {
        return repository.findResponsibleCrews(id);
    }
}
