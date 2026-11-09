package inno.ipr.ai_incident_responder_service.domain;

import java.time.Instant;

/**
 * A row of the {@code incidents} table — the persisted remediation plan plus its provenance. The
 * list fields arrive already serialised to JSON text (stored as {@code jsonb}), so the repository can
 * bind them with a {@code ?::jsonb} cast without knowing their structure.
 *
 * <p>{@link #asChildOf(String)} returns a copy linked to a root incident; the correlator uses it when
 * a root appears between the consumer's pre-check and the insert, so the plan already paid for is
 * kept but filed under the root.
 *
 * <p>{@link #isRoot()} tells whether this incident heads its own group.
 *
 * @param incidentId           unique id, e.g. {@code i-3f9c1a20}
 * @param alertId              the originating alert (unique — the idempotency key)
 * @param sensorId             sensor that tripped the alert
 * @param metricType           the breached metric
 * @param assetId              the graph asset the sensor monitors; null when the sensor is not linked
 * @param rootIncidentId       the root incident this one is a secondary effect of; null for a root
 * @param detectedAt           when the detector saw the breach; the correlation window is measured on it
 * @param criticality          severity label (Claude's, or inherited from the root)
 * @param impactedObjectsJson  JSON array of impacted objects
 * @param remediationStepsJson JSON array of remediation steps
 * @param crewsJson            JSON array of {@link Crew} resolved from the graph
 * @param summary              one-line human summary
 * @param model                the model that produced the plan, {@code mock}, or {@code correlated}
 */
public record Incident(
        String incidentId,
        String alertId,
        String sensorId,
        String metricType,
        String assetId,
        String rootIncidentId,
        Instant detectedAt,
        String criticality,
        String impactedObjectsJson,
        String remediationStepsJson,
        String crewsJson,
        String summary,
        String model) {

    public Incident asChildOf(String rootId) {
        return new Incident(incidentId, alertId, sensorId, metricType, assetId, rootId, detectedAt, criticality,
                impactedObjectsJson, remediationStepsJson, crewsJson, summary, model);
    }

    public boolean isRoot() {
        return rootIncidentId == null;
    }
}
