package inno.ipr.ai_incident_responder_service.support;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The two databases this service reads, in containers, configured like docker-compose.yml: Apache
 * AGE ({@code age-db}, AGE preloaded, extension created, the same {@code search_path} as the
 * responder's {@code connection-init-sql}) and TimescaleDB ({@code timescaledb}, where
 * {@code incidents} lives). Images match those pinned in compose and in the other services' tests.
 *
 * <p>{@link #dockerAvailable()} lets tests self-skip so {@code ./gradlew build} passes without Docker.
 *
 * <p>{@link #age()} / {@link #timescale()} create the containers; {@link #ageDataSource} and
 * {@link #timescaleDataSource} open pools on them.
 *
 * <p>{@link #seedCity(JdbcTemplate)} writes the repo's hand-seeded city into the AGE container: the
 * graph from {@code topology-service/src/main/resources/seed/city-topology.json} and the crews from
 * its {@code schema.sql}/{@code data.sql}, as topology-service would. Inlined here because the
 * responder only reads that graph and has neither topology-service's loader nor its files.
 *
 * <p>{@link #applyIncidentsSchema(HikariDataSource)} runs this service's own {@code schema.sql}, as
 * {@code spring.sql.init} does on startup.
 */
public final class Containers {

    private static final DockerImageName AGE_IMAGE = DockerImageName
            .parse("apache/age:release_PG16_1.6.0")
            .asCompatibleSubstituteFor("postgres");
    private static final DockerImageName TIMESCALE_IMAGE = DockerImageName
            .parse("timescale/timescaledb:2.28.3-pg16")
            .asCompatibleSubstituteFor("postgres");

    private Containers() {
    }

    public static boolean dockerAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    @SuppressWarnings("resource")
    public static PostgreSQLContainer age() {
        return new PostgreSQLContainer(AGE_IMAGE)
                .withDatabaseName("smartcity")
                .withUsername("smartcity")
                .withPassword("smartcity")
                .withCommand("postgres", "-c", "shared_preload_libraries=age");
    }

    @SuppressWarnings("resource")
    public static PostgreSQLContainer timescale() {
        return new PostgreSQLContainer(TIMESCALE_IMAGE)
                .withDatabaseName("smartcity")
                .withUsername("smartcity")
                .withPassword("smartcity");
    }

    public static HikariDataSource ageDataSource(PostgreSQLContainer container) {
        HikariDataSource ds = pool(container);
        ds.setConnectionInitSql("SET search_path = ag_catalog, \"$user\", public");
        new JdbcTemplate(ds).execute("CREATE EXTENSION IF NOT EXISTS age");
        return ds;
    }

    public static HikariDataSource timescaleDataSource(PostgreSQLContainer container) {
        return pool(container);
    }

    public static void applyIncidentsSchema(HikariDataSource timescale) {
        new ResourceDatabasePopulator(new ClassPathResource("schema.sql")).execute(timescale);
    }

    public static void seedCity(JdbcTemplate age) {
        age.execute("SELECT ag_catalog.create_graph('city_topology')");
        age.execute("""
                SELECT * FROM cypher('city_topology', $$
                    CREATE (sub1:Substation {id: 'sub-1', name: 'Central Substation', voltage_kv: 110}),
                           (n5:DistributionNode {id: 'node-5', name: 'Distribution Node 5'}),
                           (n6:DistributionNode {id: 'node-6', name: 'Distribution Node 6'}),
                           (b12:ResidentialBuilding {id: 'bld-12', name: 'Building 12', apartments: 84}),
                           (b19:ResidentialBuilding {id: 'bld-19', name: 'Building 19', apartments: 120}),
                           (b20:ResidentialBuilding {id: 'bld-20', name: 'Building 20', apartments: 60}),
                           (s42:Sensor {id: 's-00042', metric_type: 'pipe_pressure'}),
                           (s108:Sensor {id: 's-00108', metric_type: 'cable_temperature'}),
                           (s311:Sensor {id: 's-00311', metric_type: 'substation_voltage'}),
                           (sub1)-[:PowerLine {capacity_mw: 40}]->(n5),
                           (sub1)-[:PowerLine {capacity_mw: 40}]->(n6),
                           (n5)-[:Pipeline]->(b12),
                           (n5)-[:Pipeline]->(b19),
                           (n6)-[:Pipeline]->(b20),
                           (s42)-[:SensorLink]->(n5),
                           (s108)-[:SensorLink]->(sub1),
                           (s311)-[:SensorLink]->(sub1)
                $$) AS (v agtype)
                """);
        age.execute("""
                CREATE TABLE public.repair_crews (crew_id TEXT PRIMARY KEY, name TEXT NOT NULL, phone TEXT);
                CREATE TABLE public.crew_assignments (
                    crew_id TEXT NOT NULL REFERENCES public.repair_crews (crew_id),
                    asset_id TEXT NOT NULL,
                    PRIMARY KEY (crew_id, asset_id));
                INSERT INTO public.repair_crews VALUES
                    ('crew-hv', 'High-voltage crew', '+7-900-000-0101'),
                    ('crew-grid', 'Distribution grid crew', '+7-900-000-0102'),
                    ('crew-north', 'North district crew', '+7-900-000-0103'),
                    ('crew-south', 'South district crew', '+7-900-000-0104');
                INSERT INTO public.crew_assignments VALUES
                    ('crew-hv', 'sub-1'), ('crew-grid', 'node-5'), ('crew-grid', 'node-6'),
                    ('crew-north', 'bld-12'), ('crew-north', 'bld-19'), ('crew-south', 'bld-20');
                """);
    }

    private static HikariDataSource pool(PostgreSQLContainer container) {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(container.getJdbcUrl());
        ds.setUsername(container.getUsername());
        ds.setPassword(container.getPassword());
        ds.setMaximumPoolSize(4);
        return ds;
    }
}
