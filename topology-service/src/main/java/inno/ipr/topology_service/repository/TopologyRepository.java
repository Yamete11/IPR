package inno.ipr.topology_service.repository;

import inno.ipr.topology_service.age.AgtypeParser;
import inno.ipr.topology_service.graphql.dto.EdgeDto;
import inno.ipr.topology_service.graphql.dto.VertexDto;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.ObjectMapper;

import java.sql.Types;
import java.util.List;
import java.util.Map;

/**
 * Read side of the {@code city_topology} AGE graph. Every method runs a Cypher {@code MATCH ...
 * RETURN} as {@code SELECT * FROM cypher('city_topology', $$ ... $$) AS (...)} and maps the raw
 * agtype columns into DTOs with {@link AgtypeParser}.
 *
 * <p>Uses an <em>instance</em> {@link JdbcTemplate} (constructor-injected) — the earlier static
 * field on the service was a bug. The per-connection {@code search_path} that makes {@code cypher()}
 * resolve is set once by Hikari's {@code connection-init-sql} (see {@code application.yaml}), so no
 * per-query {@code LOAD 'age'} is needed.
 *
 * <p>The {@code id} in the dependency traversals is bound as an agtype parameter map
 * ({@code ?::agtype}, {@link Types#OTHER}) — the same injection-safe idiom {@code AgeClient.execute}
 * uses for writes — never string-interpolated into the statement.
 */
@Repository
public class TopologyRepository {

    private static final String GRAPH = "city_topology";

    /**
     * Upper bound on the dependency-walk depth. An unbounded {@code [*1..]} makes AGE enumerate every
     * simple path from the start node before {@code DISTINCT} collapses endpoints — exponential on a
     * branching/meshed graph. The city topology is shallow (substation → distribution node → building,
     * plus a sensor hop), so a bound of 10 covers it comfortably while capping worst-case cost.
     */
    private static final int MAX_DEPTH = 10;

    private final JdbcTemplate jdbc;
    private final AgtypeParser agtype;
    private final ObjectMapper objectMapper;

    public TopologyRepository(JdbcTemplate jdbc, AgtypeParser agtype, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.agtype = agtype;
        this.objectMapper = objectMapper;
    }

    public List<VertexDto> findAllVertices() {
        String sql = """
                SELECT * FROM cypher('%s', $$
                    MATCH (n)
                    RETURN n.id, label(n), properties(n)
                $$) AS (id agtype, label agtype, properties agtype)
                """.formatted(GRAPH);
        return jdbc.query(sql, vertexMapper());
    }

    public List<EdgeDto> findAllEdges() {
        String sql = """
                SELECT * FROM cypher('%s', $$
                    MATCH (a)-[r]->(b)
                    RETURN type(r), a.id, b.id, properties(r)
                $$) AS (type agtype, from_id agtype, to_id agtype, properties agtype)
                """.formatted(GRAPH);
        return jdbc.query(sql, (rs, rowNum) -> new EdgeDto(
                agtype.parseScalar(rs.getString("type")),
                agtype.parseScalar(rs.getString("from_id")),
                agtype.parseScalar(rs.getString("to_id")),
                agtype.parseMap(rs.getString("properties"))
        ));
    }

    /**
     * Everything downstream of {@code id} along the service network — the objects that lose service
     * when {@code id} fails ("all buildings depending on Distribution Node #5").
     *
     * <p>The traversal is an <em>untyped</em> variable-length walk with a {@code Sensor}-endpoint
     * filter, not a {@code [:PowerLine|Pipeline*1..]} type-union: AGE 1.6.0 supports neither a
     * relationship-type union inside a var-length match nor the {@code all()} list predicate to
     * filter path edges (both were verified to be syntax errors). This works because the graph's
     * only non-service edge, {@code SensorLink}, always runs sensor → infrastructure — so a forward
     * walk never traverses one, and {@code WHERE label(d) <> 'Sensor'} drops the only nodes a
     * SensorLink could otherwise surface. The result is exactly the PowerLine/Pipeline reachability.
     */
    public List<VertexDto> findDependents(String id) {
        String sql = """
                SELECT * FROM cypher('%s', $$
                    MATCH (n {id: $id})-[*1..%d]->(d)
                    WHERE label(d) <> 'Sensor'
                    RETURN DISTINCT d.id, label(d), properties(d)
                $$, ?::agtype) AS (id agtype, label agtype, properties agtype)
                """.formatted(GRAPH, MAX_DEPTH);
        return queryById(sql, id);
    }

    /**
     * Everything upstream of {@code id} — what {@code id} itself depends on. Same untyped-walk +
     * {@code Sensor} filter as {@link #findDependents(String)} (see there for why): a backward walk
     * can reach a sensor through its {@code SensorLink}, and the label filter drops it, leaving only
     * the upstream infrastructure.
     */
    public List<VertexDto> findDependencies(String id) {
        String sql = """
                SELECT * FROM cypher('%s', $$
                    MATCH (n {id: $id})<-[*1..%d]-(u)
                    WHERE label(u) <> 'Sensor'
                    RETURN DISTINCT u.id, label(u), properties(u)
                $$, ?::agtype) AS (id agtype, label agtype, properties agtype)
                """.formatted(GRAPH, MAX_DEPTH);
        return queryById(sql, id);
    }

    private List<VertexDto> queryById(String sql, String id) {
        String paramsJson = objectMapper.writeValueAsString(Map.of("id", id));
        return jdbc.query(sql,
                ps -> ps.setObject(1, paramsJson, Types.OTHER),
                vertexMapper());
    }

    private RowMapper<VertexDto> vertexMapper() {
        return (rs, rowNum) -> new VertexDto(
                agtype.parseScalar(rs.getString("id")),
                agtype.parseScalar(rs.getString("label")),
                agtype.parseMap(rs.getString("properties"))
        );
    }
}
