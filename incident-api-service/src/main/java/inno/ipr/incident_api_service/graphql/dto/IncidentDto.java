package inno.ipr.incident_api_service.graphql.dto;

import java.util.List;

/**
 * GraphQL projection of one {@code incidents} row. Component names match the {@code Incident} fields
 * in {@code schema.graphqls}, so Spring for GraphQL maps them automatically. The two JSONB array
 * columns are already parsed into {@code List<String>} by the repository, and {@code createdAt} is an
 * ISO-8601 string.
 */
public record IncidentDto(
        String incidentId,
        String alertId,
        String sensorId,
        String metricType,
        String criticality,
        List<String> impactedObjects,
        List<String> remediationSteps,
        String summary,
        String model,
        String createdAt) {
}
