package inno.ipr.incident_api_service.graphql.dto;

import java.util.List;

/**
 * GraphQL projection of one {@code incidents} row. Component names match the {@code Incident} fields
 * in {@code schema.graphqls}, so Spring for GraphQL maps them automatically. The JSONB columns are
 * already parsed by the repository ({@code List<String>}, {@code List<CrewDto>}), and timestamps are
 * ISO-8601 strings. {@code rootIncident} and {@code linkedIncidents} are not components: the
 * controller resolves them per incident with {@code @SchemaMapping}, only when a query asks.
 *
 * <p>{@code assetId}, {@code rootIncidentId} and {@code detectedAt} are null on rows written before
 * root-cause correlation existed, and {@code crews} is empty.
 */
public record IncidentDto(
        String incidentId,
        String alertId,
        String sensorId,
        String metricType,
        String assetId,
        String rootIncidentId,
        String detectedAt,
        String criticality,
        List<String> impactedObjects,
        List<String> remediationSteps,
        List<CrewDto> crews,
        String summary,
        String model,
        String createdAt) {
}
