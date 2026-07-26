package inno.ipr.ai_incident_responder_service.domain;

/**
 * A row of the {@code incidents} table — the persisted remediation plan plus its provenance. The
 * two list fields arrive already serialised to JSON text (stored as {@code jsonb}), so the
 * repository can bind them with a {@code ?::jsonb} cast without knowing their structure.
 *
 * @param incidentId          unique id, e.g. {@code i-3f9c1a20}
 * @param alertId             the originating alert (unique — the idempotency key)
 * @param sensorId            sensor that tripped the alert
 * @param metricType          the breached metric
 * @param criticality         Claude's severity label
 * @param impactedObjectsJson JSON array of impacted objects
 * @param remediationStepsJson JSON array of remediation steps
 * @param summary             one-line human summary
 * @param model               the Claude model that produced the plan
 */
public record Incident(
        String incidentId,
        String alertId,
        String sensorId,
        String metricType,
        String criticality,
        String impactedObjectsJson,
        String remediationStepsJson,
        String summary,
        String model) {
}
