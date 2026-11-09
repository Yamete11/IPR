package inno.ipr.ai_incident_responder_service.correlation;

import inno.ipr.ai_incident_responder_service.domain.GraphContext;
import inno.ipr.ai_incident_responder_service.domain.Incident;
import inno.ipr.ai_incident_responder_service.repository.IncidentRepository;
import inno.ipr.ai_incident_responder_service.repository.IncidentRepository.RootIncident;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Root-cause correlation over the AGE dependency graph: an alert on an asset whose upstream already
 * has an active incident is a secondary effect of it and is filed under that incident instead of
 * getting its own plan and its own Claude call. "Active" means detected within ±{@code ai.correlation.window}
 * (default 30 minutes): the detector publishes no recovery events, so time is the only signal. It is
 * a heuristic — co-occurrence along a dependency path, not proof of cause — documented as such in
 * docs/system-overview.md. Gated by {@code ai.correlation.enabled}.
 *
 * <p>{@link #enabled()} tells the consumer whether to fetch the upstream walk at all.
 *
 * <p>{@link #findRoot(GraphContext, Instant)} is the consumer's pre-check, run outside any transaction
 * and before the planner, purely to skip the Claude call for a secondary alert.
 *
 * <p>{@link #persist(Incident, GraphContext)} stores the incident in one transaction on the
 * TimescaleDB datasource, under {@link IncidentRepository#lockCorrelation()} so concurrent alerts
 * cannot both become roots of one outage. Only fast SQL runs under the lock, never the Claude call.
 * A root incident re-checks for a root that appeared since the pre-check (and if so keeps its paid
 * plan but files it as a child), is inserted, and then adopts the roots already open downstream
 * (alerts that arrived before their cause). A lost {@code ON CONFLICT} race returns
 * {@link Outcome#stored()} false and adopts nothing.
 *
 * <p>{@link Outcome} reports what happened, for the consumer's log line.
 */
@Component
public class IncidentCorrelator {

    private final IncidentRepository incidents;
    private final TransactionTemplate tx;
    private final boolean enabled;
    private final Duration window;

    public IncidentCorrelator(IncidentRepository incidents,
                              @Qualifier("timescaleDataSource") DataSource timescale,
                              @Value("${ai.correlation.enabled:true}") boolean enabled,
                              @Value("${ai.correlation.window:30m}") Duration window) {
        this.incidents = incidents;
        this.tx = new TransactionTemplate(new DataSourceTransactionManager(timescale));
        this.enabled = enabled;
        this.window = window;
    }

    public record Outcome(boolean stored, String rootIncidentId, int adopted) {
    }

    public boolean enabled() {
        return enabled;
    }

    public Optional<RootIncident> findRoot(GraphContext graph, Instant detectedAt) {
        if (!enabled || !graph.linked()) {
            return Optional.empty();
        }
        return incidents.findActiveRoot(graph.upstreamRanked(), detectedAt, window);
    }

    public Outcome persist(Incident incident, GraphContext graph) {
        return tx.execute(status -> {
            incidents.lockCorrelation();
            Incident toStore = incident;
            if (toStore.isRoot()) {
                Optional<RootIncident> late = findRoot(graph, incident.detectedAt());
                if (late.isPresent()) {
                    toStore = incident.asChildOf(late.get().incidentId());
                }
            }
            if (incidents.insert(toStore) == 0) {
                return new Outcome(false, toStore.rootIncidentId(), 0);
            }
            int adopted = 0;
            if (toStore.isRoot() && enabled && graph.linked()) {
                adopted = incidents.adopt(toStore.incidentId(), graph.impactedIds(), incident.detectedAt(), window);
            }
            return new Outcome(true, toStore.rootIncidentId(), adopted);
        });
    }
}
