package inno.ipr.ai_incident_responder_service.repository;

import inno.ipr.ai_incident_responder_service.age.AgtypeParser;
import inno.ipr.ai_incident_responder_service.domain.ImpactedObject;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.ObjectMapper;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Read side of the {@code city_topology} AGE graph (owned/seeded by topology-service). Answers the
 * "what does this failing sensor take down?" question that enriches an alert:
 *
 * <ol>
 *   <li>{@link #findMonitoredAsset(String)} — the infrastructure vertex the sensor is attached to
 *       (via a {@code SensorLink} edge);</li>
 *   <li>{@link #findDependents(String)} — everything downstream of that asset along the service
 *       network, reusing topology-service's untyped-walk + {@code Sensor}-filter idiom.</li>
 * </ol>
 *
 * <p>{@link #findImpacted(String)} combines them: the asset plus its dependents. Uses the
 * <em>age-db</em> {@code JdbcTemplate} (its Hikari {@code connection-init-sql} puts
 * {@code ag_catalog} on the search_path). The {@code id} is bound as an agtype parameter map
 * ({@code ?::agtype}, {@link Types#OTHER}) — never string-interpolated.
 */
@Repository
public class GraphRepository {

    private static final String GRAPH = "city_topology";

    /** Upper bound on the dependency walk depth; the city topology is shallow (see topology-service). */
    private static final int MAX_DEPTH = 10;

    private final JdbcTemplate jdbc;
    private final AgtypeParser agtype;
    private final ObjectMapper objectMapper;

    public GraphRepository(@Qualifier("ageJdbc") JdbcTemplate jdbc,
                           AgtypeParser agtype,
                           ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.agtype = agtype;
        this.objectMapper = objectMapper;
    }

    /**
     * The asset the sensor monitors, plus everything downstream that loses service if it fails.
     * Empty when the sensor is not in the graph (unknown/synthetic sensor).
     */
    public List<ImpactedObject> findImpacted(String sensorId) {
        Optional<ImpactedObject> asset = findMonitoredAsset(sensorId);
        if (asset.isEmpty()) {
            return List.of();
        }
        List<ImpactedObject> impacted = new ArrayList<>();
        impacted.add(asset.get());
        impacted.addAll(findDependents(asset.get().id()));
        return impacted;
    }

    /** The infrastructure vertex reached from the sensor's outgoing {@code SensorLink}. */
    public Optional<ImpactedObject> findMonitoredAsset(String sensorId) {
        String sql = """
                SELECT * FROM cypher('%s', $$
                    MATCH (s:Sensor {id: $id})-[:SensorLink]->(a)
                    RETURN a.id, label(a), properties(a)
                $$, ?::agtype) AS (id agtype, label agtype, properties agtype)
                """.formatted(GRAPH);
        return queryById(sql, sensorId).stream().findFirst();
    }

    /** Everything downstream of {@code assetId} along PowerLine/Pipeline reachability. */
    public List<ImpactedObject> findDependents(String assetId) {
        String sql = """
                SELECT * FROM cypher('%s', $$
                    MATCH (n {id: $id})-[*1..%d]->(d)
                    WHERE label(d) <> 'Sensor'
                    RETURN DISTINCT d.id, label(d), properties(d)
                $$, ?::agtype) AS (id agtype, label agtype, properties agtype)
                """.formatted(GRAPH, MAX_DEPTH);
        return queryById(sql, assetId);
    }

    private List<ImpactedObject> queryById(String sql, String id) {
        String paramsJson = objectMapper.writeValueAsString(Map.of("id", id));
        return jdbc.query(sql,
                ps -> ps.setObject(1, paramsJson, Types.OTHER),
                mapper());
    }

    private RowMapper<ImpactedObject> mapper() {
        return (rs, rowNum) -> new ImpactedObject(
                agtype.parseScalar(rs.getString("id")),
                agtype.parseScalar(rs.getString("label")),
                agtype.parseMap(rs.getString("properties")));
    }
}
