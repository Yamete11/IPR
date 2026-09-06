package inno.ipr.ai_incident_responder_service.repository;

import inno.ipr.ai_incident_responder_service.domain.Incident;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Write side of the {@code incidents} table (TimescaleDB) — the only table this service owns. Plain
 * JDBC, an instance {@link JdbcTemplate}, mirroring the other services' repository shape.
 *
 * <p>{@link #existsByAlertId(String)} + {@code ON CONFLICT (alert_id) DO NOTHING} implement the
 * Idempotent Consumer pattern: the consumer checks first to avoid a redundant (billed) Claude call,
 * and the unique constraint is the backstop if two deliveries race.
 */
@Repository
public class IncidentRepository {

    private static final String EXISTS =
            "SELECT EXISTS(SELECT 1 FROM incidents WHERE alert_id = ?)";

    private static final String INSERT = """
            INSERT INTO incidents (incident_id, alert_id, sensor_id, metric_type, criticality,
                                   impacted_objects, remediation_steps, summary, model)
            VALUES (?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?)
            ON CONFLICT (alert_id) DO NOTHING
            """;

    private final JdbcTemplate jdbc;

    public IncidentRepository(@Qualifier("timescaleJdbc") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public boolean existsByAlertId(String alertId) {
        return Boolean.TRUE.equals(jdbc.queryForObject(EXISTS, Boolean.class, alertId));
    }

    /**
     * @return 1 if the incident was stored, 0 if a row for this {@code alert_id} already existed and
     *         {@code ON CONFLICT DO NOTHING} suppressed the insert. Callers must not ignore the 0:
     *         it means a redelivery raced past {@link #existsByAlertId(String)} and the plan just
     *         produced — a billed Claude call — was discarded.
     */
    public int save(Incident incident) {
        return jdbc.update(INSERT,
                incident.incidentId(),
                incident.alertId(),
                incident.sensorId(),
                incident.metricType(),
                incident.criticality(),
                incident.impactedObjectsJson(),
                incident.remediationStepsJson(),
                incident.summary(),
                incident.model());
    }
}
