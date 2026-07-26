package inno.ipr.ai_incident_responder_service.consumer;

import inno.ipr.ai_incident_responder_service.ai.IncidentPlan;
import inno.ipr.ai_incident_responder_service.ai.IncidentPlanner;
import inno.ipr.ai_incident_responder_service.alert.Alert;
import inno.ipr.ai_incident_responder_service.domain.ImpactedObject;
import inno.ipr.ai_incident_responder_service.domain.Incident;
import inno.ipr.ai_incident_responder_service.domain.MetricSummary;
import inno.ipr.ai_incident_responder_service.exception.InvalidAlertException;
import inno.ipr.ai_incident_responder_service.repository.GraphRepository;
import inno.ipr.ai_incident_responder_service.repository.IncidentRepository;
import inno.ipr.ai_incident_responder_service.repository.MetricHistoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.UUID;

/**
 * Consumes {@code infrastructure-alerts} and drives the incident pipeline for each alert:
 * parse → idempotency check → gather context (metric history + impacted graph objects) →
 * Claude remediation plan → persist to {@code incidents}.
 *
 * <p>The payload arrives as a {@code String}; parse/validation failures throw
 * {@link InvalidAlertException} which the container's error handler dead-letters immediately, while
 * transient Claude/DB failures propagate and are retried (see {@code KafkaConfig}). The idempotency
 * check short-circuits redelivered alerts <em>before</em> any billed Claude call.
 */
@Component
public class AlertConsumer {

    private static final Logger log = LoggerFactory.getLogger(AlertConsumer.class);

    private final ObjectMapper objectMapper;
    private final IncidentRepository incidents;
    private final MetricHistoryRepository metrics;
    private final GraphRepository graph;
    private final IncidentPlanner planner;

    public AlertConsumer(ObjectMapper objectMapper,
                         IncidentRepository incidents,
                         MetricHistoryRepository metrics,
                         GraphRepository graph,
                         IncidentPlanner planner) {
        this.objectMapper = objectMapper;
        this.incidents = incidents;
        this.metrics = metrics;
        this.graph = graph;
        this.planner = planner;
    }

    @KafkaListener(topics = "infrastructure-alerts", groupId = "ai-incident-responder")
    public void onMessage(String payload) {
        Alert alert = parse(payload);

        if (incidents.existsByAlertId(alert.alertId())) {
            log.info("Alert {} already has an incident — skipping (idempotent)", alert.alertId());
            return;
        }

        MetricSummary history = metrics.findLastHour(alert.sensorId());
        List<ImpactedObject> impacted = graph.findImpacted(alert.sensorId());
        log.info("Handling alert {} (sensor {}, {}): {} readings/1h, {} impacted objects",
                alert.alertId(), alert.sensorId(), alert.metricType(), history.count(), impacted.size());

        IncidentPlan plan = planner.plan(alert, history, impacted);

        Incident incident = new Incident(
                "i-" + UUID.randomUUID().toString().substring(0, 8),
                alert.alertId(),
                alert.sensorId(),
                alert.metricType(),
                plan.criticality(),
                toJson(plan.impactedObjects()),
                toJson(plan.remediationSteps()),
                plan.summary(),
                planner.model());
        incidents.save(incident);
        log.info("Persisted incident {} for alert {} (criticality {})",
                incident.incidentId(), alert.alertId(), plan.criticality());
    }

    private Alert parse(String payload) {
        Alert alert;
        try {
            alert = objectMapper.readValue(payload, Alert.class);
        } catch (RuntimeException e) {
            throw new InvalidAlertException("Malformed alert JSON", payload, e);
        }
        if (isBlank(alert.alertId())) {
            throw new InvalidAlertException("Missing 'alert_id'", payload);
        }
        if (isBlank(alert.sensorId())) {
            throw new InvalidAlertException("Missing 'sensor_id'", payload);
        }
        return alert;
    }

    /** Serialises a list to JSON for a {@code jsonb} column; a null list becomes {@code []}. */
    private String toJson(List<String> list) {
        return objectMapper.writeValueAsString(list == null ? List.of() : list);
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
