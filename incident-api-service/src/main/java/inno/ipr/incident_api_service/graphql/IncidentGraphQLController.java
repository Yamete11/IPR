package inno.ipr.incident_api_service.graphql;

import inno.ipr.incident_api_service.graphql.dto.IncidentDto;
import inno.ipr.incident_api_service.graphql.dto.MetricDto;
import inno.ipr.incident_api_service.repository.IncidentReadRepository;
import inno.ipr.incident_api_service.repository.MetricReadRepository;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

import java.util.List;

/**
 * Read-only GraphQL API for the demo dashboard over the {@code incidents} table and TimescaleDB
 * {@code metrics}. Method names match the {@code Query} fields in {@code schema.graphqls}; each
 * delegates to a read repository. Stateless — no writes, no Kafka. {@code limit} arguments carry
 * their schema defaults, so they are always supplied by Spring for GraphQL.
 *
 * <p>{@link #clamp(int)} bounds every client-supplied {@code limit} to {@code [0, MAX_LIMIT]}
 * ({@code MAX_LIMIT} = 1000) before it reaches {@code LIMIT ?}. The schema defaults are only
 * defaults — a caller may send any {@code Int}, and both extremes are hostile against a hypertable:
 * a huge limit pulls an unbounded result set into heap, while a negative one is a Postgres error
 * rather than an empty page.
 */
@Controller
public class IncidentGraphQLController {

    private static final int MAX_LIMIT = 1000;

    private final IncidentReadRepository incidentRepository;
    private final MetricReadRepository metricRepository;

    public IncidentGraphQLController(IncidentReadRepository incidentRepository,
                                     MetricReadRepository metricRepository) {
        this.incidentRepository = incidentRepository;
        this.metricRepository = metricRepository;
    }

    @QueryMapping
    public List<IncidentDto> incidents(@Argument int limit) {
        return incidentRepository.findRecent(clamp(limit));
    }

    @QueryMapping
    public IncidentDto incident(@Argument String id) {
        return incidentRepository.findById(id);
    }

    @QueryMapping
    public List<IncidentDto> incidentsBySensor(@Argument String sensorId, @Argument int limit) {
        return incidentRepository.findBySensor(sensorId, clamp(limit));
    }

    @QueryMapping
    public List<MetricDto> recentMetrics(@Argument String sensorId, @Argument int limit) {
        return metricRepository.findRecentBySensor(sensorId, clamp(limit));
    }

    private static int clamp(int limit) {
        return Math.clamp(limit, 0, MAX_LIMIT);
    }
}
