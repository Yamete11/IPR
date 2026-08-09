package inno.ipr.topology_service.repository;

import inno.ipr.topology_service.age.AgtypeParser;
import inno.ipr.topology_service.graphql.dto.EdgeDto;
import inno.ipr.topology_service.graphql.dto.VertexDto;
import org.springframework.beans.factory.annotation.Value;
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

    /** Same property {@code SeedLoader} writes to — see there for why they must agree. */
    private final String graph;

    public TopologyRepository(JdbcTemplate jdbc,
                              AgtypeParser agtype,
                              ObjectMapper objectMapper,
                              @Value("${topology.graph:city_topology}") String graph) {
        this.jdbc = jdbc;
        this.agtype = agtype;
        this.objectMapper = objectMapper;
        this.graph = graph;
    }

    /**
     * <p>The {@code id IS NOT NULL} guard keeps the non-null GraphQL contract honest. {@code Vertex.id}
     * is {@code ID!} inside a {@code [Vertex!]!} list, so a single id-less vertex — this graph is a
     * shared database and nothing constrains writers to set one — would not merely drop its own row:
     * the non-null violation bubbles up and nulls the whole {@code vertices} field. Skipping such
     * vertices returns the well-formed subset instead of failing the query outright.
     */
    public List<VertexDto> findAllVertices() {
        String sql = """
                SELECT * FROM cypher('%s', $$
                    MATCH (n)
                    WHERE n.id IS NOT NULL
                    RETURN n.id, label(n), properties(n)
                $$) AS (id agtype, label agtype, properties agtype)
                """.formatted(graph);
        return jdbc.query(sql, vertexMapper());
    }

    /** Same {@code IS NOT NULL} reasoning as {@link #findAllVertices()}: {@code Edge.from}/{@code to} are {@code ID!}. */
    public List<EdgeDto> findAllEdges() {
        String sql = """
                SELECT * FROM cypher('%s', $$
                    MATCH (a)-[r]->(b)
                    WHERE a.id IS NOT NULL AND b.id IS NOT NULL
                    RETURN type(r), a.id, b.id, properties(r)
                $$) AS (type agtype, from_id agtype, to_id agtype, properties agtype)
                """.formatted(graph);
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
     * <p>The traversal is an <em>untyped</em> variable-length walk with {@code Sensor} label filters,
     * not a {@code [:PowerLine|Pipeline*1..]} type-union: AGE 1.6.0 supports neither a
     * relationship-type union inside a var-length match nor the {@code all()} list predicate to
     * filter path edges (both were verified to be syntax errors). Two filters are needed, because
     * the endpoint filter alone does not stop the walk from *traversing* the graph's one non-service
     * edge, {@code SensorLink}:
     *
     * <ul>
     *   <li>{@code label(n) <> 'Sensor'} on the <em>start</em> node. A SensorLink runs
     *       sensor → infrastructure, so starting at a sensor crosses one on the very first hop:
     *       without this, {@code dependents('s-00042')} returns node-5 and the buildings behind it,
     *       claiming they lose service when a sensor dies. A sensor has no dependents — a failed
     *       sensor takes nothing down — so the correct answer is an empty list.</li>
     *   <li>{@code label(d) <> 'Sensor'} on the endpoint, which drops sensors hanging off an
     *       infrastructure node.</li>
     * </ul>
     *
     * <p>With both, no path can include a SensorLink — every one of them starts at a sensor, which
     * is now excluded as a start node and unreachable mid-walk (sensors have no incoming edges) —
     * so the result is exactly the PowerLine/Pipeline reachability.
     */
    public List<VertexDto> findDependents(String id) {
        String sql = """
                SELECT * FROM cypher('%s', $$
                    MATCH (n {id: $id})-[*1..%d]->(d)
                    WHERE label(n) <> 'Sensor' AND label(d) <> 'Sensor'
                    RETURN DISTINCT d.id, label(d), properties(d)
                $$, ?::agtype) AS (id agtype, label agtype, properties agtype)
                """.formatted(graph, MAX_DEPTH);
        return queryById(sql, id);
    }

    /**
     * Everything upstream of {@code id} — what {@code id} itself depends on. Same untyped walk as
     * {@link #findDependents(String)}, but the endpoint filter is sufficient here and no start-node
     * filter is needed: a {@code SensorLink} runs sensor → infrastructure, so walking <em>backwards</em>
     * can only ever arrive <em>at</em> a sensor, never continue through one (sensors have no incoming
     * edges). {@code label(u) <> 'Sensor'} drops those arrivals, leaving the upstream infrastructure.
     * A sensor start node needs no special case — it has no incoming edges, so the walk is empty.
     */
    public List<VertexDto> findDependencies(String id) {
        String sql = """
                SELECT * FROM cypher('%s', $$
                    MATCH (n {id: $id})<-[*1..%d]-(u)
                    WHERE label(u) <> 'Sensor'
                    RETURN DISTINCT u.id, label(u), properties(u)
                $$, ?::agtype) AS (id agtype, label agtype, properties agtype)
                """.formatted(graph, MAX_DEPTH);
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
