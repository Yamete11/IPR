package inno.ipr.incident_api_service.graphql.dto;

/**
 * GraphQL projection of one {@code metrics} row for the dashboard's per-sensor charts. Component
 * names match the {@code Metric} fields in {@code schema.graphqls}; {@code timestamp} is an ISO-8601
 * string.
 */
public record MetricDto(
        String timestamp,
        String sensorId,
        String metricType,
        double value,
        String status) {
}
