package inno.ipr.ai_incident_responder_service.repository;

import com.zaxxer.hikari.HikariDataSource;
import inno.ipr.ai_incident_responder_service.age.AgtypeParser;
import inno.ipr.ai_incident_responder_service.domain.Crew;
import inno.ipr.ai_incident_responder_service.domain.GraphContext;
import inno.ipr.ai_incident_responder_service.domain.ImpactedObject;
import inno.ipr.ai_incident_responder_service.support.Containers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link GraphRepository} against a real Apache AGE holding the hand-seeded city. Covers what the
 * mocked consumer tests cannot: the {@code [*0..3]} walk with the zero-length path, the hybrid
 * {@code cypher() LEFT JOIN} onto the crew tables, the backwards walk and its ranking, and the
 * deterministic ordering the prompt relies on.
 *
 * <p>Self-skips when Docker is unavailable so {@code ./gradlew build} still passes without it.
 */
@Testcontainers
@EnabledIf("inno.ipr.ai_incident_responder_service.support.Containers#dockerAvailable")
class GraphRepositoryIntegrationTest {

    @Container
    private static final PostgreSQLContainer AGE = Containers.age();

    private static HikariDataSource dataSource;
    private static GraphRepository graph;

    @BeforeAll
    static void seed() {
        dataSource = Containers.ageDataSource(AGE);
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        Containers.seedCity(jdbc);
        JsonMapper mapper = JsonMapper.builder().build();
        graph = new GraphRepository(jdbc, new AgtypeParser(mapper), mapper);
    }

    @AfterAll
    static void close() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @Test
    @DisplayName("a node sensor resolves its node, the buildings behind it, their crews and the substation above")
    void nodeSensor() {
        GraphContext context = graph.loadContext("s-00042", true);

        assertThat(context.asset().id()).isEqualTo("node-5");
        assertThat(context.impacted()).extracting(ImpactedObject::id).containsExactly("node-5", "bld-12", "bld-19");
        assertThat(context.impacted().get(1).properties()).containsEntry("apartments", 84);
        assertThat(context.crews()).containsExactly(
                new Crew("crew-grid", "Distribution grid crew", "+7-900-000-0102", List.of("node-5")),
                new Crew("crew-north", "North district crew", "+7-900-000-0103", List.of("bld-12", "bld-19")));
        assertThat(context.upstreamRanked()).containsExactly("sub-1", "node-5");
    }

    @Test
    @DisplayName("a substation sensor takes down the whole subtree, ordered by tier, with every crew below")
    void substationSensor() {
        GraphContext context = graph.loadContext("s-00311", true);

        assertThat(context.impacted()).extracting(ImpactedObject::id)
                .containsExactly("sub-1", "node-5", "node-6", "bld-12", "bld-19", "bld-20");
        assertThat(context.crews()).extracting(Crew::crewId)
                .containsExactly("crew-grid", "crew-hv", "crew-north", "crew-south");
        assertThat(context.upstreamRanked()).containsExactly("sub-1");
    }

    @Test
    @DisplayName("without correlation the upstream walk is not run")
    void noUpstreamWhenNotAsked() {
        assertThat(graph.loadContext("s-00042", false).upstreamRanked()).isEmpty();
    }

    @Test
    @DisplayName("a sensor that is not in the graph has no context")
    void unknownSensor() {
        GraphContext context = graph.loadContext("s-99999", true);

        assertThat(context.linked()).isFalse();
        assertThat(context.impacted()).isEmpty();
        assertThat(context.crews()).isEmpty();
    }
}
