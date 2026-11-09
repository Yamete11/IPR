package inno.ipr.ai_incident_responder_service.consumer;

import inno.ipr.ai_incident_responder_service.ai.IncidentContext;
import inno.ipr.ai_incident_responder_service.ai.IncidentPlan;
import inno.ipr.ai_incident_responder_service.ai.IncidentPlanner;
import inno.ipr.ai_incident_responder_service.alert.Alert;
import inno.ipr.ai_incident_responder_service.config.KafkaConfig;
import inno.ipr.ai_incident_responder_service.correlation.IncidentCorrelator;
import inno.ipr.ai_incident_responder_service.domain.GraphContext;
import inno.ipr.ai_incident_responder_service.domain.Incident;
import inno.ipr.ai_incident_responder_service.domain.MetricSummary;
import inno.ipr.ai_incident_responder_service.exception.InvalidAlertException;
import inno.ipr.ai_incident_responder_service.repository.GraphRepository;
import inno.ipr.ai_incident_responder_service.repository.IncidentRepository;
import inno.ipr.ai_incident_responder_service.repository.IncidentRepository.RootIncident;
import inno.ipr.ai_incident_responder_service.repository.MetricHistoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Consumes {@code infrastructure-alerts} and drives the incident pipeline for each alert:
 * parse → idempotency check → gather context (metric history from TimescaleDB; monitored asset,
 * impacted objects, responsible crews and upstream assets from the AGE graph) → root-cause check →
 * Claude remediation plan, or a correlated child of an existing incident → persist to
 * {@code incidents}.
 *
 * <p>The payload arrives as a {@code String}; parse/validation failures throw
 * {@link InvalidAlertException} which the container's error handler dead-letters immediately, while
 * transient Claude/DB failures propagate and are retried (see {@code KafkaConfig}). The idempotency
 * check short-circuits redelivered alerts <em>before</em> any billed Claude call.
 *
 * <p>{@link #onMessage(String)} asks {@link IncidentCorrelator#findRoot} whether an incident upstream
 * of (or on) the alert's asset is still active. If so the alert is a secondary effect: {@code child}
 * builds its incident without calling the planner — criticality inherited from the root, a fixed
 * summary and steps pointing at the root, {@code model = "correlated"} — which is where the graph
 * saves a Claude call per downstream alert of one outage. Otherwise the planner runs as before. Either
 * way the correlator persists it, which may still turn a planned root into a child (a root appeared
 * meanwhile) or let a new root adopt incidents already open downstream. An outcome with nothing stored
 * means {@code ON CONFLICT (alert_id) DO NOTHING} swallowed the row: a redelivery slipped past the
 * {@code existsByAlertId} check, and the plan just produced was discarded.
 *
 * <p>{@code planned} turns a plan into an incident; impacted objects and steps come from the plan,
 * crews always from the graph (structured data, never the model's text).
 *
 * <p>{@code parse(String)} wraps only Jackson's own failures ({@code JacksonException}) as
 * {@link InvalidAlertException}, because only those mean "the detector sent us garbage". Jackson 3
 * exceptions are all unchecked, so catching {@code RuntimeException} would also swallow our own
 * bugs and dead-letter a perfectly good alert under a misleading "malformed JSON" label.
 * {@code toJson(Object)} serialises a value for a {@code jsonb} column; a null list becomes
 * {@code []}. {@code detected_at} is required because it anchors the metric-history window and the
 * correlation window.
 *
 * <p>The consumer group id comes from {@code spring.kafka.consumer.group-id}, so it can be
 * overridden per environment.
 */
@Component
public class AlertConsumer {

    private static final Logger log = LoggerFactory.getLogger(AlertConsumer.class);

    private final ObjectMapper objectMapper;
    private final IncidentRepository incidents;
    private final MetricHistoryRepository metrics;
    private final GraphRepository graph;
    private final IncidentPlanner planner;
    private final IncidentCorrelator correlator;

    public AlertConsumer(ObjectMapper objectMapper,
                         IncidentRepository incidents,
                         MetricHistoryRepository metrics,
                         GraphRepository graph,
                         IncidentPlanner planner,
                         IncidentCorrelator correlator) {
        this.objectMapper = objectMapper;
        this.incidents = incidents;
        this.metrics = metrics;
        this.graph = graph;
        this.planner = planner;
        this.correlator = correlator;
    }

    @KafkaListener(topics = KafkaConfig.ALERTS_TOPIC)
    public void onMessage(String payload) {
        Alert alert = parse(payload);

        if (incidents.existsByAlertId(alert.alertId())) {
            log.info("Alert {} already has an incident — skipping (idempotent)", alert.alertId());
            return;
        }

        MetricSummary history = metrics.findLastHour(alert.sensorId(), alert.metricType(), alert.detectedAt());
        GraphContext context = graph.loadContext(alert.sensorId(), correlator.enabled());
        log.info("Handling alert {} (sensor {}, {}): {} readings/1h, asset {}, {} impacted objects, {} crews",
                alert.alertId(), alert.sensorId(), alert.metricType(), history.count(),
                context.linked() ? context.asset().id() : "none", context.impacted().size(), context.crews().size());

        Optional<RootIncident> root = correlator.findRoot(context, alert.detectedAt());
        Incident incident = root.isPresent()
                ? child(alert, context, root.get())
                : planned(alert, context,
                        planner.plan(new IncidentContext(alert, history, context.impacted(), context.crews())));

        IncidentCorrelator.Outcome outcome = correlator.persist(incident, context);
        if (!outcome.stored()) {
            log.warn("Alert {} was already persisted by a concurrent delivery — discarding the incident "
                    + "produced for it", alert.alertId());
            return;
        }
        if (outcome.rootIncidentId() != null) {
            log.info("Persisted incident {} for alert {} as a secondary effect of {} (model {})",
                    incident.incidentId(), alert.alertId(), outcome.rootIncidentId(), incident.model());
        } else {
            log.info("Persisted incident {} for alert {} (criticality {}); adopted {} downstream incidents",
                    incident.incidentId(), alert.alertId(), incident.criticality(), outcome.adopted());
        }
    }

    private Incident planned(Alert alert, GraphContext context, IncidentPlan plan) {
        return new Incident(
                "i-" + UUID.randomUUID(),
                alert.alertId(),
                alert.sensorId(),
                alert.metricType(),
                context.linked() ? context.asset().id() : null,
                null,
                alert.detectedAt(),
                plan.criticality(),
                toJson(plan.impactedObjects()),
                toJson(plan.remediationSteps()),
                toJson(context.crews()),
                plan.summary(),
                planner.model());
    }

    private Incident child(Alert alert, GraphContext context, RootIncident root) {
        String asset = context.asset().id();
        List<String> objects = context.impacted().stream().map(o -> o.label() + " " + o.id()).toList();
        List<String> steps = List.of(
                "Handled under root incident " + root.incidentId() + " on " + root.assetId()
                        + "; no separate dispatch.",
                "Verify " + asset + " (" + alert.metricType() + " at sensor " + alert.sensorId()
                        + ") once the root cause is cleared.");
        String summary = "Secondary effect of incident " + root.incidentId() + " on " + root.assetId() + ": "
                + alert.metricType() + " breach on " + alert.sensorId() + " at " + asset + ".";
        return new Incident(
                "i-" + UUID.randomUUID(),
                alert.alertId(),
                alert.sensorId(),
                alert.metricType(),
                asset,
                root.incidentId(),
                alert.detectedAt(),
                root.criticality(),
                toJson(objects),
                toJson(steps),
                toJson(context.crews()),
                summary,
                "correlated");
    }

    private Alert parse(String payload) {
        Alert alert;
        try {
            alert = objectMapper.readValue(payload, Alert.class);
        } catch (JacksonException e) {
            throw new InvalidAlertException("Malformed alert JSON", payload, e);
        }
        if (isBlank(alert.alertId())) {
            throw new InvalidAlertException("Missing 'alert_id'", payload);
        }
        if (isBlank(alert.sensorId())) {
            throw new InvalidAlertException("Missing 'sensor_id'", payload);
        }
        if (alert.detectedAt() == null) {
            throw new InvalidAlertException("Missing 'detected_at'", payload);
        }
        return alert;
    }

    private String toJson(Object value) {
        return objectMapper.writeValueAsString(value == null ? List.of() : value);
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
