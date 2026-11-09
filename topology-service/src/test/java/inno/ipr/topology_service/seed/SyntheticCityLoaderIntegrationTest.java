package inno.ipr.topology_service.seed;

import com.zaxxer.hikari.HikariDataSource;
import inno.ipr.topology_service.age.AgeClient;
import inno.ipr.topology_service.age.AgtypeParser;
import inno.ipr.topology_service.graphql.dto.CrewDto;
import inno.ipr.topology_service.graphql.dto.VertexDto;
import inno.ipr.topology_service.repository.TopologyRepository;
import inno.ipr.topology_service.support.AgeContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link SyntheticCityLoader} against a real Apache AGE, next to the hand seed, as the app runs it.
 * Proves the AGE 1.6 dialect the loader relies on ({@code UNWIND ... CREATE} with a property map,
 * labeled {@code MATCH ... CREATE ... RETURN 1}, filtered {@code DETACH DELETE}) and that the city is
 * queryable through the same {@link TopologyRepository} the dashboard uses.
 *
 * <p>The methods run in order on one container: first load, then a no-op reload, then a smaller city
 * that must replace the first one. 60 sensors give districts {@code d00} (16 buildings) and {@code d01}.
 *
 * <p>Self-skips when Docker is unavailable so {@code ./gradlew build} still passes without it.
 */
@Testcontainers
@EnabledIf("inno.ipr.topology_service.support.AgeContainer#dockerAvailable")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SyntheticCityLoaderIntegrationTest {

    private static final String GRAPH = "city_topology";

    @Container
    private static final PostgreSQLContainer AGE = AgeContainer.create();

    private static HikariDataSource dataSource;
    private static JdbcTemplate jdbc;
    private static AgeClient age;
    private static SyntheticCityLoader loader;
    private static TopologyRepository repository;

    @BeforeAll
    static void setUp() throws Exception {
        dataSource = AgeContainer.dataSource(AGE);
        jdbc = new JdbcTemplate(dataSource);
        new ResourceDatabasePopulator(new ClassPathResource("schema.sql"), new ClassPathResource("data.sql"))
                .execute(dataSource);
        JsonMapper mapper = JsonMapper.builder().build();
        age = new AgeClient(jdbc);
        new SeedLoader(age, mapper, new DefaultResourceLoader(),
                "classpath:seed/city-topology.json", GRAPH, true).run(null);
        loader = new SyntheticCityLoader(age, jdbc, new TransactionTemplate(new DataSourceTransactionManager(dataSource)),
                mapper, GRAPH);
        repository = new TopologyRepository(jdbc, new AgtypeParser(mapper), mapper, GRAPH);
    }

    @AfterAll
    static void close() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @Test
    @Order(1)
    void loadsTheCityNextToTheHandSeed() {
        SyntheticCity city = SyntheticCity.generate(60);

        assertThat(loader.load(city)).isTrue();

        assertThat(cityVertices()).isEqualTo(city.vertexCount());
        assertThat(age.count(GRAPH, "MATCH ()-[r]->() RETURN count(r)")).isEqualTo(8 + city.edgeCount());
        assertThat(repository.findDependents("sub-d00")).extracting(VertexDto::id)
                .hasSize(20).contains("node-d00-0", "bld-d00-3-3");
        assertThat(repository.findResponsibleCrews("bld-d00-0-0")).extracting(CrewDto::crewId)
                .containsExactly("crew-d00");
        assertThat(repository.findDependents("node-5")).extracting(VertexDto::id)
                .containsExactlyInAnyOrder("bld-12", "bld-19");
    }

    @Test
    @Order(2)
    void sameCityIsNotLoadedTwice() {
        assertThat(loader.load(SyntheticCity.generate(60))).isFalse();

        assertThat(cityVertices()).isEqualTo(SyntheticCity.generate(60).vertexCount());
    }

    @Test
    @Order(3)
    void changedCityReplacesThePreviousOne() {
        SyntheticCity smaller = SyntheticCity.generate(30);

        assertThat(loader.load(smaller)).isTrue();

        assertThat(cityVertices()).isEqualTo(smaller.vertexCount());
        assertThat(repository.findDependents("sub-d01")).isEmpty();
        assertThat(jdbc.queryForList("SELECT crew_id FROM public.repair_crews WHERE crew_id LIKE 'crew-d%'",
                String.class)).isEqualTo(List.of("crew-d00"));
        assertThat(repository.findDependents("node-5")).extracting(VertexDto::id)
                .containsExactlyInAnyOrder("bld-12", "bld-19");
    }

    private static long cityVertices() {
        return age.count(GRAPH, "MATCH (n) WHERE n.district IS NOT NULL RETURN count(n)");
    }
}
