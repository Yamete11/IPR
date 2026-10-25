package inno.ipr.topology_service.repository;

import com.zaxxer.hikari.HikariDataSource;
import inno.ipr.topology_service.age.AgeClient;
import inno.ipr.topology_service.age.AgtypeParser;
import inno.ipr.topology_service.graphql.dto.CrewDto;
import inno.ipr.topology_service.graphql.dto.ImpactSummaryDto;
import inno.ipr.topology_service.graphql.dto.ImpactSummaryDto.LabelCountDto;
import inno.ipr.topology_service.graphql.dto.PathDto;
import inno.ipr.topology_service.graphql.dto.VertexDto;
import inno.ipr.topology_service.seed.SeedLoader;
import inno.ipr.topology_service.support.AgeContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The read side against a real Apache AGE seeded by the real {@link SeedLoader} from
 * {@code seed/city-topology.json}, plus the crews tables from {@code schema.sql}/{@code data.sql}.
 * Covers what no mock can: AGE's Cypher dialect (aggregation, path binding, {@code nodes(p)}
 * rendering), the agtype ↔ SQL join, and whether the property index is actually used.
 *
 * <p>{@code impactSummary}: a sensor takes nothing down (same rule as dependents). {@code paths}:
 * a path through a SensorLink is found, and none against the edge direction.
 * {@code propertyIndexIsUsed}: the seed graph is tiny, so the planner would rightly prefer a seq
 * scan; disabling it shows whether the index is <em>usable</em> for AGE's property predicate at all.
 * The index has a mixed-case name, so EXPLAIN prints it quoted, and AGE compiles the property map in
 * the pattern to agtype containment ({@code @>}), which GIN supports.
 *
 * <p>Self-skips when Docker is unavailable so {@code ./gradlew build} still passes without it.
 */
@Testcontainers
@EnabledIf("inno.ipr.topology_service.support.AgeContainer#dockerAvailable")
class TopologyRepositoryIntegrationTest {

    private static final String GRAPH = "city_topology";

    @Container
    private static final PostgreSQLContainer AGE = AgeContainer.create();

    private static HikariDataSource dataSource;
    private static JdbcTemplate jdbc;
    private static TopologyRepository repository;

    @BeforeAll
    static void seed() throws Exception {
        dataSource = AgeContainer.dataSource(AGE);
        jdbc = new JdbcTemplate(dataSource);
        new ResourceDatabasePopulator(new ClassPathResource("schema.sql"), new ClassPathResource("data.sql"))
                .execute(dataSource);

        JsonMapper mapper = JsonMapper.builder().build();
        new SeedLoader(new AgeClient(jdbc), mapper, new DefaultResourceLoader(),
                "classpath:seed/city-topology.json", GRAPH, true).run(null);
        repository = new TopologyRepository(jdbc, new AgtypeParser(mapper), mapper, GRAPH);
    }

    @AfterAll
    static void close() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @Test
    @DisplayName("dependents: buildings fed by node-5")
    void dependents() {
        assertThat(repository.findDependents("node-5")).extracting(VertexDto::id)
                .containsExactlyInAnyOrder("bld-12", "bld-19");
    }

    @Test
    @DisplayName("aggregation: count per label and summed apartments")
    void impactSummary() {
        ImpactSummaryDto node5 = repository.impactSummary("node-5");
        assertThat(node5.totalObjects()).isEqualTo(2);
        assertThat(node5.apartments()).isEqualTo(84 + 120);
        assertThat(node5.byLabel()).containsExactly(new LabelCountDto("ResidentialBuilding", 2));

        ImpactSummaryDto substation = repository.impactSummary("sub-1");
        assertThat(substation.totalObjects()).isEqualTo(5);
        assertThat(substation.apartments()).isEqualTo(84 + 120 + 60);
        assertThat(substation.byLabel()).containsExactly(
                new LabelCountDto("DistributionNode", 2), new LabelCountDto("ResidentialBuilding", 3));

        assertThat(repository.impactSummary("s-00042").totalObjects()).isZero();
    }

    @Test
    @DisplayName("path finding: whole path bound and parsed from nodes(p)")
    void paths() {
        List<PathDto> paths = repository.findPaths("sub-1", "bld-12", 5);
        assertThat(paths).hasSize(1);
        assertThat(paths.getFirst().length()).isEqualTo(2);
        assertThat(paths.getFirst().nodes()).extracting(VertexDto::id).containsExactly("sub-1", "node-5", "bld-12");
        assertThat(paths.getFirst().nodes()).extracting(VertexDto::label)
                .containsExactly("Substation", "DistributionNode", "ResidentialBuilding");
        assertThat(paths.getFirst().nodes().get(2).properties()).containsEntry("apartments", 84);

        assertThat(repository.findPaths("s-00042", "bld-19", 5)).singleElement()
                .extracting(PathDto::length).isEqualTo(2);
        assertThat(repository.findPaths("bld-12", "sub-1", 5)).isEmpty();
    }

    @Test
    @DisplayName("hybrid SQL + Cypher: crews joined to the dependents walk")
    void responsibleCrews() {
        assertThat(repository.findResponsibleCrews("node-5")).containsExactly(
                new CrewDto("crew-grid", "Distribution grid crew", "+7-900-000-0102", List.of("node-5")),
                new CrewDto("crew-north", "North district crew", "+7-900-000-0103", List.of("bld-12", "bld-19")));
        assertThat(repository.findResponsibleCrews("sub-1")).extracting(CrewDto::crewId)
                .containsExactly("crew-grid", "crew-hv", "crew-north", "crew-south");
        assertThat(repository.findResponsibleCrews("s-00042")).isEmpty();
    }

    @Test
    @DisplayName("index: an {id: ...} match is a GIN index probe on the label table")
    void propertyIndexIsUsed() {
        List<String> plan = jdbc.execute((ConnectionCallback<List<String>>) conn -> {
            try (var st = conn.createStatement()) {
                st.execute("SET enable_seqscan = off");
                List<String> lines = new ArrayList<>();
                try (ResultSet rs = st.executeQuery("EXPLAIN SELECT * FROM cypher('" + GRAPH
                        + "', $$ MATCH (n:DistributionNode {id: 'node-5'}) RETURN n $$) AS (n agtype)")) {
                    while (rs.next()) {
                        lines.add(rs.getString(1));
                    }
                }
                st.execute("RESET enable_seqscan");
                return lines;
            }
        });
        String text = String.join("\n", plan);
        assertThat(text).contains("Bitmap Index Scan on \"" + GRAPH + "_DistributionNode_props_gin\"");
        assertThat(text).contains("properties @> '{\"id\": \"node-5\"}'::agtype");
    }

    @Test
    @DisplayName("index: start_id/end_id indexed on every edge type (AGE creates none)")
    void edgeIndexesExist() {
        List<String> indexes = jdbc.queryForList(
                "SELECT indexname FROM pg_indexes WHERE schemaname = ? ORDER BY indexname", String.class, GRAPH);
        for (String type : List.of("PowerLine", "Pipeline", "SensorLink")) {
            assertThat(indexes).contains(GRAPH + "_" + type + "_start_id", GRAPH + "_" + type + "_end_id");
        }
    }
}
