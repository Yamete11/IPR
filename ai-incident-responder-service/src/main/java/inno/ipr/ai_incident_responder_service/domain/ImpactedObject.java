package inno.ipr.ai_incident_responder_service.domain;

import java.util.Map;

/**
 * A vertex from the AGE topology graph affected by a failing sensor — the monitored asset itself or
 * something downstream of it. Fed to Claude as part of the incident context.
 *
 * @param id         graph id, e.g. {@code node-5} / {@code bld-12}
 * @param label      vertex label, e.g. {@code DistributionNode} / {@code ResidentialBuilding}
 * @param properties the vertex's agtype properties parsed to JSON (e.g. {@code {"name": "..."}})
 */
public record ImpactedObject(String id, String label, Map<String, Object> properties) {
}
