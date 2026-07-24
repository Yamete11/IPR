package inno.ipr.topology_service.service;

import inno.ipr.topology_service.graphql.dto.EdgeDto;
import inno.ipr.topology_service.graphql.dto.VertexDto;
import inno.ipr.topology_service.repository.TopologyRepository;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Application-service layer the GraphQL controller calls. Thin by design: it delegates the read
 * queries to {@link TopologyRepository}, which owns the Cypher and the agtype → DTO mapping.
 */
@Service
public class TopologyQueryService {

    private final TopologyRepository repository;

    public TopologyQueryService(TopologyRepository repository) {
        this.repository = repository;
    }

    public List<VertexDto> findAllVertices() {
        return repository.findAllVertices();
    }

    public List<EdgeDto> findAllEdges() {
        return repository.findAllEdges();
    }

    public List<VertexDto> findDependents(String id) {
        return repository.findDependents(id);
    }

    public List<VertexDto> findDependencies(String id) {
        return repository.findDependencies(id);
    }
}
