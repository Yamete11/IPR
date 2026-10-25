package inno.ipr.topology_service.repository;

import inno.ipr.topology_service.age.AgtypeParser;
import inno.ipr.topology_service.graphql.dto.CrewDto;
import inno.ipr.topology_service.graphql.dto.EdgeDto;
import inno.ipr.topology_service.graphql.dto.ImpactSummaryDto;
import inno.ipr.topology_service.graphql.dto.PathDto;
import inno.ipr.topology_service.graphql.dto.VertexDto;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.ObjectMapper;

import java.sql.Types;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Read side of the {@code city_topology} AGE graph. Every method runs a Cypher {@code MATCH ...
 * RETURN} as {@code SELECT * FROM cypher('city_topology', $$ ... $$) AS (...)} and maps the raw
 * agtype columns into DTOs with {@link AgtypeParser}.
 *
 * <p>The per-connection {@code search_path} that makes {@code cypher()} resolve is set once by
 * Hikari's {@code connection-init-sql} (see {@code application.yaml}), so no per-query
 * {@code LOAD 'age'} is needed.
 *
 * <p>Ids are never string-interpolated: {@code agtypeParams(Map)} serialises them into the agtype
 * parameter map bound to {@code ?::agtype} as {@link Types#OTHER} — the same injection-safe idiom
 * {@code AgeClient.execute} uses for writes. {@code graph} is the same {@code topology.graph} property {@code SeedLoader} writes to,
 * which is why the loader rejects a seed file whose graph name disagrees with it.
 *
 * <p>{@code MAX_DEPTH} (10) is the upper bound on the dependency-walk depth. An unbounded
 * {@code [*1..]} makes AGE enumerate every simple path from the start node before {@code DISTINCT}
 * collapses endpoints — exponential on a branching/meshed graph. The city topology is shallow
 * (substation → distribution node → building, plus a sensor hop), so a bound of 10 covers it
 * comfortably while capping worst-case cost. {@code MAX_PATHS} (20) bounds {@code paths(limit:)},
 * since each path is a separate enumerated walk.
 *
 * <p>{@link #findAllVertices()}: the {@code id IS NOT NULL} guard keeps the non-null GraphQL
 * contract honest. {@code Vertex.id} is {@code ID!} inside a {@code [Vertex!]!} list, so a single
 * id-less vertex — this graph is a shared database and nothing constrains writers to set one —
 * would not merely drop its own row: the non-null violation bubbles up and nulls the whole
 * {@code vertices} field. Skipping such vertices returns the well-formed subset instead of failing
 * the query outright. {@link #findAllEdges()} applies the same guard to both endpoints, because
 * {@code Edge.from}/{@code to} are {@code ID!}.
 *
 * <p>{@link #findDependents(String)} returns everything downstream of {@code id} along the service
 * network — the objects that lose service when {@code id} fails ("all buildings depending on
 * Distribution Node #5"). The traversal is an <em>untyped</em> variable-length walk with
 * {@code Sensor} label filters, not a {@code [:PowerLine|Pipeline*1..]} type-union: AGE 1.6.0
 * supports neither a relationship-type union inside a var-length match nor the {@code all()} list
 * predicate to filter path edges (both were verified to be syntax errors). Two filters are needed,
 * because the endpoint filter alone does not stop the walk from *traversing* the graph's one
 * non-service edge, {@code SensorLink}:
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
 *
 * <p>{@link #findDependencies(String)} returns everything upstream of {@code id} — what {@code id}
 * itself depends on. Same untyped walk, but the endpoint filter is sufficient here and no
 * start-node filter is needed: a {@code SensorLink} runs sensor → infrastructure, so walking
 * <em>backwards</em> can only ever arrive <em>at</em> a sensor, never continue through one (sensors
 * have no incoming edges). {@code label(u) <> 'Sensor'} drops those arrivals, leaving the upstream
 * infrastructure. A sensor start node needs no special case — it has no incoming edges, so the walk
 * is empty.
 *
 * <p>{@link #impactSummary(String)} is Cypher aggregation over the same walk as
 * {@link #findDependents(String)}: {@code WITH DISTINCT d} collapses the many paths to one row per
 * dependent, then {@code count}/{@code sum} with {@code label(d)} as the only non-aggregate column
 * group by label (Cypher's implicit GROUP BY). {@code coalesce} because only buildings carry
 * {@code apartments}. Totals are summed in Java from the per-label rows.
 *
 * <p>{@link #findPaths(String, String, int)} does path finding: every directed path from
 * {@code from} to {@code to}, shortest first. AGE 1.6.0 has no {@code shortestPath()}, so the
 * bounded variable-length match binds the whole path {@code p} and {@code ORDER BY length(p)} ranks
 * them. No label filters — a path from a sensor through its SensorLink to a building is a
 * legitimate answer here. {@code nodes(p)} is returned whole (list comprehensions over it fail in
 * AGE 1.6.0) and parsed by {@link AgtypeParser#parseVertexList}; {@code toVertex(Map)} then maps
 * each entity, taking the business id from its properties rather than AGE's graphid.
 *
 * <p>{@link #findResponsibleCrews(String)} is hybrid SQL + Cypher: the Cypher walk (the asset itself
 * via {@code *0..}, plus everything downstream) is a row source in an ordinary SQL statement, joined
 * to the relational {@code repair_crews}/{@code crew_assignments} tables and grouped per crew.
 * {@code agtype::text} unwraps an agtype string to plain text ({@code "node-5"} → {@code node-5})
 * for the join key. The tables are schema-qualified because the connection's search_path starts
 * with {@code ag_catalog}. A sensor start node yields no rows, as in {@link #findDependents(String)}.
 */
@Repository
public class TopologyRepository {

    private static final int MAX_DEPTH = 10;

    private static final int MAX_PATHS = 20;

    private final JdbcTemplate jdbc;
    private final AgtypeParser agtype;
    private final ObjectMapper objectMapper;

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

    public ImpactSummaryDto impactSummary(String id) {
        String sql = """
                SELECT * FROM cypher('%s', $$
                    MATCH (n {id: $id})-[*1..%d]->(d)
                    WHERE label(n) <> 'Sensor' AND label(d) <> 'Sensor'
                    WITH DISTINCT d
                    RETURN label(d), count(d), sum(coalesce(d.apartments, 0))
                $$, ?::agtype) AS (label agtype, objects agtype, apartments agtype)
                """.formatted(graph, MAX_DEPTH);
        record Row(String label, long objects, long apartments) {
        }
        List<Row> rows = jdbc.query(sql,
                agtypeParams(Map.of("id", id)),
                (rs, rowNum) -> new Row(
                        agtype.parseScalar(rs.getString("label")),
                        agtype.parseLong(rs.getString("objects")),
                        agtype.parseLong(rs.getString("apartments"))));
        List<ImpactSummaryDto.LabelCountDto> byLabel = rows.stream()
                .sorted(Comparator.comparing(Row::label))
                .map(r -> new ImpactSummaryDto.LabelCountDto(r.label(), r.objects()))
                .toList();
        return new ImpactSummaryDto(id,
                rows.stream().mapToLong(Row::objects).sum(),
                rows.stream().mapToLong(Row::apartments).sum(),
                byLabel);
    }

    public List<PathDto> findPaths(String from, String to, int limit) {
        int bounded = Math.clamp(limit, 1, MAX_PATHS);
        String sql = """
                SELECT * FROM cypher('%s', $$
                    MATCH p = (a {id: $from})-[*1..%d]->(b {id: $to})
                    RETURN nodes(p), length(p)
                    ORDER BY length(p)
                    LIMIT %d
                $$, ?::agtype) AS (nodes agtype, length agtype)
                """.formatted(graph, MAX_DEPTH, bounded);
        return jdbc.query(sql,
                agtypeParams(Map.of("from", from, "to", to)),
                (rs, rowNum) -> new PathDto(
                        Math.toIntExact(agtype.parseLong(rs.getString("length"))),
                        agtype.parseVertexList(rs.getString("nodes")).stream().map(this::toVertex).toList()));
    }

    public List<CrewDto> findResponsibleCrews(String id) {
        String sql = """
                SELECT c.crew_id, c.name, c.phone, array_agg(a.asset_id ORDER BY a.asset_id) AS assets
                FROM cypher('%s', $$
                    MATCH (n {id: $id})-[*0..%d]->(d)
                    WHERE label(n) <> 'Sensor' AND label(d) <> 'Sensor'
                    RETURN DISTINCT d.id
                $$, ?::agtype) AS g(asset_id agtype)
                JOIN public.crew_assignments a ON a.asset_id = g.asset_id::text
                JOIN public.repair_crews c ON c.crew_id = a.crew_id
                GROUP BY c.crew_id, c.name, c.phone
                ORDER BY c.crew_id
                """.formatted(graph, MAX_DEPTH);
        return jdbc.query(sql,
                agtypeParams(Map.of("id", id)),
                (rs, rowNum) -> new CrewDto(
                        rs.getString("crew_id"),
                        rs.getString("name"),
                        rs.getString("phone"),
                        List.of((String[]) rs.getArray("assets").getArray())));
    }

    private List<VertexDto> queryById(String sql, String id) {
        return jdbc.query(sql, agtypeParams(Map.of("id", id)), vertexMapper());
    }

    private PreparedStatementSetter agtypeParams(Map<String, ?> params) {
        String json = objectMapper.writeValueAsString(params);
        return ps -> ps.setObject(1, json, Types.OTHER);
    }

    @SuppressWarnings("unchecked")
    private VertexDto toVertex(Map<String, Object> entity) {
        Map<String, Object> props = (Map<String, Object>) entity.getOrDefault("properties", Map.of());
        return new VertexDto(String.valueOf(props.get("id")), String.valueOf(entity.get("label")), props);
    }

    private RowMapper<VertexDto> vertexMapper() {
        return (rs, rowNum) -> new VertexDto(
                agtype.parseScalar(rs.getString("id")),
                agtype.parseScalar(rs.getString("label")),
                agtype.parseMap(rs.getString("properties"))
        );
    }
}
