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
 * "what does this failing sensor take down?" question that enriches an alert. Uses the
 * <em>age-db</em> {@code JdbcTemplate} (its Hikari {@code connection-init-sql} puts
 * {@code ag_catalog} on the search_path). The {@code id} is bound as an agtype parameter map
 * ({@code ?::agtype}, {@link Types#OTHER}) — never string-interpolated.
 *
 * <p>{@link #findImpacted(String)} returns the asset the sensor monitors plus everything downstream
 * that loses service if it fails, and is empty when the sensor is not in the graph
 * (unknown/synthetic sensor).
 *
 * <p>{@code findMonitoredAsset(String)} returns the infrastructure vertex reached from the sensor's
 * outgoing {@code SensorLink} edge.
 *
 * <p>{@code findDependents(String)} returns everything downstream of the asset along
 * PowerLine/Pipeline reachability, reusing topology-service's untyped-walk + {@code Sensor}-filter
 * idiom, with the walk depth bounded by {@code MAX_DEPTH} (10 — the city topology is shallow, see
 * topology-service).
 */
@Repository
public class GraphRepository {

    private static final String GRAPH = "city_topology";

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

    private Optional<ImpactedObject> findMonitoredAsset(String sensorId) {
        String sql = """
                SELECT * FROM cypher('%s', $$
                    MATCH (s:Sensor {id: $id})-[:SensorLink]->(a)
                    RETURN a.id, label(a), properties(a)
                $$, ?::agtype) AS (id agtype, label agtype, properties agtype)
                """.formatted(GRAPH);
        return queryById(sql, sensorId).stream().findFirst();
    }

    private List<ImpactedObject> findDependents(String assetId) {
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
