package inno.ipr.ai_incident_responder_service.repository;

import inno.ipr.ai_incident_responder_service.age.AgtypeParser;
import inno.ipr.ai_incident_responder_service.domain.Crew;
import inno.ipr.ai_incident_responder_service.domain.GraphContext;
import inno.ipr.ai_incident_responder_service.domain.ImpactedObject;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.ObjectMapper;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Read side of the {@code city_topology} AGE graph (owned and seeded by topology-service). Answers
 * three questions about a failing sensor: which asset does it watch, what fails with that asset and
 * who repairs it, and what does that asset depend on. Uses the <em>age-db</em> {@code JdbcTemplate}
 * (its Hikari {@code connection-init-sql} puts {@code ag_catalog} on the search_path). Ids are bound
 * as an agtype parameter map ({@code ?::agtype}, {@link Types#OTHER}), never interpolated; the one
 * interpolated token, the asset's label, comes from the graph and is checked against {@code LABEL}.
 *
 * <p>{@link #loadContext(String, boolean)} runs the queries below and assembles a {@link GraphContext};
 * a sensor with no {@code SensorLink} yields {@link GraphContext#none()}. Two variable-length walks
 * at most per alert, the second only when correlation needs it.
 *
 * <p>{@link #findMonitoredAsset(String)} follows the sensor's single outgoing {@code SensorLink}.
 *
 * <p>{@code findImpact} is one hybrid statement: the Cypher walk {@code (n)-[*0..3]->(d)} returns the
 * asset itself (the zero-length path) and everything downstream, and plain SQL {@code LEFT JOIN}s the
 * relational crew tables onto it. One row per (asset, crew), folded here into the impacted list and
 * the crew list. {@code HIERARCHY_DEPTH} is 3: the longest service chain is Substation → Node →
 * Building (2 hops), and AGE's variable-length executor gets slower with the bound, not only the
 * answer (docs/graph-vs-sql.md). Impacted objects are ordered asset first, then by {@code LABEL_RANK}
 * and id, so the prompt is deterministic.
 *
 * <p>{@code findUpstream} walks the edges backwards ({@code (n)<-[*1..3]-(u)}), ranks the result most
 * upstream first and appends the asset itself: the order in which correlation prefers a root.
 *
 * <p>Both walks start from {@code (n:Label {id: $id})} with the asset's label, so the start vertex is
 * a probe of one label table instead of an {@code Append} over all of them.
 */
@Repository
public class GraphRepository {

    private static final String GRAPH = "city_topology";
    private static final int HIERARCHY_DEPTH = 3;
    private static final Pattern LABEL = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Map<String, Integer> LABEL_RANK =
            Map.of("Substation", 0, "DistributionNode", 1, "ResidentialBuilding", 2);

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

    public GraphContext loadContext(String sensorId, boolean withUpstream) {
        Optional<ImpactedObject> asset = findMonitoredAsset(sensorId);
        if (asset.isEmpty()) {
            return GraphContext.none();
        }
        Impact impact = findImpact(asset.get());
        List<String> upstream = withUpstream ? findUpstream(asset.get()) : List.of();
        return new GraphContext(asset.get(), impact.objects(), impact.crews(), upstream);
    }

    public Optional<ImpactedObject> findMonitoredAsset(String sensorId) {
        String sql = """
                SELECT * FROM cypher('%s', $$
                    MATCH (s:Sensor {id: $id})-[:SensorLink]->(a)
                    RETURN a.id, label(a), properties(a)
                $$, ?::agtype) AS (id agtype, label agtype, properties agtype)
                """.formatted(GRAPH);
        return jdbc.query(sql, ps -> ps.setObject(1, params(sensorId), Types.OTHER),
                (rs, rowNum) -> new ImpactedObject(
                        agtype.parseScalar(rs.getString("id")),
                        agtype.parseScalar(rs.getString("label")),
                        agtype.parseMap(rs.getString("properties")))).stream().findFirst();
    }

    private Impact findImpact(ImpactedObject asset) {
        String sql = """
                SELECT g.id, g.label, g.properties, c.crew_id, c.name, c.phone
                FROM cypher('%s', $$
                    MATCH (n:%s {id: $id})-[*0..%d]->(d)
                    WHERE label(d) <> 'Sensor'
                    RETURN DISTINCT d.id, label(d), properties(d)
                $$, ?::agtype) AS g(id agtype, label agtype, properties agtype)
                LEFT JOIN public.crew_assignments a ON a.asset_id = g.id::text
                LEFT JOIN public.repair_crews c ON c.crew_id = a.crew_id
                """.formatted(GRAPH, label(asset), HIERARCHY_DEPTH);
        Map<String, ImpactedObject> objects = new LinkedHashMap<>();
        Map<String, Crew> crews = new LinkedHashMap<>();
        Map<String, TreeSet<String>> crewAssets = new LinkedHashMap<>();
        jdbc.query(sql, ps -> ps.setObject(1, params(asset.id()), Types.OTHER), rs -> {
            String id = agtype.parseScalar(rs.getString("id"));
            String label = rs.getString("label");
            String properties = rs.getString("properties");
            objects.computeIfAbsent(id, k -> new ImpactedObject(k,
                    agtype.parseScalar(label), agtype.parseMap(properties)));
            String crewId = rs.getString("crew_id");
            if (crewId != null) {
                crews.putIfAbsent(crewId, new Crew(crewId, rs.getString("name"), rs.getString("phone"), List.of()));
                crewAssets.computeIfAbsent(crewId, k -> new TreeSet<>()).add(id);
            }
        });
        List<ImpactedObject> ordered = new ArrayList<>(objects.values());
        ordered.sort(Comparator.comparing((ImpactedObject o) -> !o.id().equals(asset.id()))
                .thenComparing(o -> LABEL_RANK.getOrDefault(o.label(), LABEL_RANK.size()))
                .thenComparing(ImpactedObject::id));
        if (ordered.isEmpty()) {
            ordered.add(asset);
        }
        List<Crew> crewList = crews.values().stream()
                .map(c -> new Crew(c.crewId(), c.name(), c.phone(), List.copyOf(crewAssets.get(c.crewId()))))
                .sorted(Comparator.comparing(Crew::crewId))
                .toList();
        return new Impact(List.copyOf(ordered), crewList);
    }

    private List<String> findUpstream(ImpactedObject asset) {
        String sql = """
                SELECT * FROM cypher('%s', $$
                    MATCH (n:%s {id: $id})<-[*1..%d]-(u)
                    WHERE label(u) <> 'Sensor'
                    RETURN DISTINCT u.id, label(u)
                $$, ?::agtype) AS (id agtype, label agtype)
                """.formatted(GRAPH, label(asset), HIERARCHY_DEPTH);
        List<ImpactedObject> upstream = jdbc.query(sql, ps -> ps.setObject(1, params(asset.id()), Types.OTHER),
                (rs, rowNum) -> new ImpactedObject(
                        agtype.parseScalar(rs.getString("id")),
                        agtype.parseScalar(rs.getString("label")),
                        Map.of()));
        List<String> ranked = new ArrayList<>(upstream.stream()
                .sorted(Comparator.comparing((ImpactedObject o) -> LABEL_RANK.getOrDefault(o.label(), LABEL_RANK.size()))
                        .thenComparing(ImpactedObject::id))
                .map(ImpactedObject::id)
                .toList());
        ranked.add(asset.id());
        return List.copyOf(ranked);
    }

    private String params(String id) {
        return objectMapper.writeValueAsString(Map.of("id", id));
    }

    private static String label(ImpactedObject asset) {
        if (asset.label() == null || !LABEL.matcher(asset.label()).matches()) {
            throw new IllegalStateException("Unexpected vertex label from the graph: " + asset.label());
        }
        return asset.label();
    }

    private record Impact(List<ImpactedObject> objects, List<Crew> crews) {
    }
}
