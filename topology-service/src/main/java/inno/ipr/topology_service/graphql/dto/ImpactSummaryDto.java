package inno.ipr.topology_service.graphql.dto;

import java.util.List;

/**
 * Aggregate of everything that loses service if {@code sourceId} fails: the Cypher groups the
 * dependents by label (implicit {@code GROUP BY label(d)}) and sums the buildings' apartments.
 */
public record ImpactSummaryDto(String sourceId, long totalObjects, long apartments, List<LabelCountDto> byLabel) {

    public record LabelCountDto(String label, long count) {
    }
}
