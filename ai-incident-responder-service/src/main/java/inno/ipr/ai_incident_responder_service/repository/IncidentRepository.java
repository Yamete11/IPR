package inno.ipr.ai_incident_responder_service.repository;

import inno.ipr.ai_incident_responder_service.domain.Incident;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Write side of the {@code incidents} table (TimescaleDB) — the only table this service owns. Plain
 * JDBC, an instance {@link JdbcTemplate}, mirroring the other services' repository shape.
 *
 * <p>{@link #existsByAlertId(String)} + {@code ON CONFLICT (alert_id) DO NOTHING} implement the
 * Idempotent Consumer pattern: the consumer checks first to avoid a redundant (billed) Claude call,
 * and the unique constraint is the backstop if two deliveries race.
 *
 * <p>{@link #insert(Incident)} returns 1 if the incident was stored, 0 if a row for this
 * {@code alert_id} already existed and {@code ON CONFLICT DO NOTHING} suppressed the insert.
 * Callers must not ignore the 0: it means a redelivery raced past {@link #existsByAlertId(String)}.
 *
 * <p>{@link #findActiveRoot(List, Instant, Duration)} is root-cause correlation's lookup: the root
 * incident (one with no {@code root_incident_id}) on any of the given assets whose {@code detected_at}
 * lies within ±window of the new alert's. Both directions, because the detector publishes a poll's
 * alerts in sensor-id order, so a building's alert often arrives before its substation's. Ties go to
 * the asset earliest in {@code rankedAssetIds} (most upstream first), then the earliest detection.
 *
 * <p>{@link #adopt(String, List, Instant, Duration)} files under a new root every root incident on
 * the given (downstream) assets within the window, together with those roots' own children, in one
 * statement: the subquery reads the snapshot from before the update, so groups stay one level deep.
 * Returns the number of incidents moved.
 *
 * <p>{@link #lockCorrelation()} takes a transaction-scoped advisory lock serialising the
 * check-insert-adopt sequence across replicas; it must run inside a transaction.
 *
 * <p>{@code textArray} binds a list as a {@code text[]} for {@code = ANY(?)} and
 * {@code array_position}; the window is bound as an interval of whole seconds.
 *
 * <p>{@link RootIncident} is what correlation needs from a root: its id, inherited criticality and
 * the asset it sits on.
 */
@Repository
public class IncidentRepository {

    private static final String EXISTS =
            "SELECT EXISTS(SELECT 1 FROM incidents WHERE alert_id = ?)";

    private static final String INSERT = """
            INSERT INTO incidents (incident_id, alert_id, sensor_id, metric_type, asset_id, root_incident_id,
                                   detected_at, criticality, impacted_objects, remediation_steps, crews,
                                   summary, model)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?::jsonb, ?, ?)
            ON CONFLICT (alert_id) DO NOTHING
            """;

    private static final String FIND_ACTIVE_ROOT = """
            SELECT incident_id, criticality, asset_id
            FROM incidents
            WHERE asset_id = ANY(?)
              AND root_incident_id IS NULL
              AND detected_at BETWEEN ?::timestamptz - ?::interval AND ?::timestamptz + ?::interval
            ORDER BY array_position(?, asset_id), detected_at
            LIMIT 1
            """;

    private static final String ADOPT = """
            UPDATE incidents SET root_incident_id = ?
            WHERE incident_id <> ?
              AND (
                (asset_id = ANY(?) AND root_incident_id IS NULL
                 AND detected_at BETWEEN ?::timestamptz - ?::interval AND ?::timestamptz + ?::interval)
                OR root_incident_id IN (
                  SELECT incident_id FROM incidents
                  WHERE asset_id = ANY(?) AND root_incident_id IS NULL
                    AND detected_at BETWEEN ?::timestamptz - ?::interval AND ?::timestamptz + ?::interval))
            """;

    public record RootIncident(String incidentId, String criticality, String assetId) {
    }

    private final JdbcTemplate jdbc;

    public IncidentRepository(@Qualifier("timescaleJdbc") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public boolean existsByAlertId(String alertId) {
        return Boolean.TRUE.equals(jdbc.queryForObject(EXISTS, Boolean.class, alertId));
    }

    public int insert(Incident incident) {
        return jdbc.update(INSERT,
                incident.incidentId(),
                incident.alertId(),
                incident.sensorId(),
                incident.metricType(),
                incident.assetId(),
                incident.rootIncidentId(),
                incident.detectedAt() == null ? null : Timestamp.from(incident.detectedAt()),
                incident.criticality(),
                incident.impactedObjectsJson(),
                incident.remediationStepsJson(),
                incident.crewsJson() == null ? "[]" : incident.crewsJson(),
                incident.summary(),
                incident.model());
    }

    public Optional<RootIncident> findActiveRoot(List<String> rankedAssetIds, Instant detectedAt, Duration window) {
        if (rankedAssetIds.isEmpty()) {
            return Optional.empty();
        }
        Timestamp at = Timestamp.from(detectedAt);
        String interval = interval(window);
        return jdbc.query(con -> {
            PreparedStatement ps = con.prepareStatement(FIND_ACTIVE_ROOT);
            ps.setArray(1, textArray(con, rankedAssetIds));
            ps.setTimestamp(2, at);
            ps.setString(3, interval);
            ps.setTimestamp(4, at);
            ps.setString(5, interval);
            ps.setArray(6, textArray(con, rankedAssetIds));
            return ps;
        }, (rs, rowNum) -> new RootIncident(
                rs.getString("incident_id"), rs.getString("criticality"), rs.getString("asset_id")))
                .stream().findFirst();
    }

    public int adopt(String rootId, List<String> assetIds, Instant detectedAt, Duration window) {
        if (assetIds.isEmpty()) {
            return 0;
        }
        Timestamp at = Timestamp.from(detectedAt);
        String interval = interval(window);
        return jdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(ADOPT);
            ps.setString(1, rootId);
            ps.setString(2, rootId);
            ps.setArray(3, textArray(con, assetIds));
            ps.setTimestamp(4, at);
            ps.setString(5, interval);
            ps.setTimestamp(6, at);
            ps.setString(7, interval);
            ps.setArray(8, textArray(con, assetIds));
            ps.setTimestamp(9, at);
            ps.setString(10, interval);
            ps.setTimestamp(11, at);
            ps.setString(12, interval);
            return ps;
        });
    }

    public void lockCorrelation() {
        jdbc.queryForObject("SELECT pg_advisory_xact_lock(hashtext('incident-correlation'))", Object.class);
    }

    private static Array textArray(Connection con, List<String> values) throws SQLException {
        return con.createArrayOf("text", values.toArray());
    }

    private static String interval(Duration window) {
        return window.toSeconds() + " seconds";
    }
}
