package inno.ipr.topology_service.support;

import com.zaxxer.hikari.HikariDataSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Apache AGE in a container, configured like {@code age-db} in the repo's docker-compose.yml:
 * same image, AGE preloaded, extension created, and a pool whose connections get the same
 * {@code search_path} as topology-service's Hikari {@code connection-init-sql}. {@code IMAGE}
 * matches the image pinned in docker-compose.yml.
 */
public final class AgeContainer {

    private static final DockerImageName IMAGE = DockerImageName
            .parse("apache/age:release_PG16_1.6.0")
            .asCompatibleSubstituteFor("postgres");

    private AgeContainer() {
    }

    public static boolean dockerAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    @SuppressWarnings("resource")
    public static PostgreSQLContainer create() {
        return new PostgreSQLContainer(IMAGE)
                .withDatabaseName("smartcity")
                .withUsername("smartcity")
                .withPassword("smartcity")
                .withCommand("postgres", "-c", "shared_preload_libraries=age");
    }

    public static HikariDataSource dataSource(PostgreSQLContainer container) {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(container.getJdbcUrl());
        ds.setUsername(container.getUsername());
        ds.setPassword(container.getPassword());
        ds.setMaximumPoolSize(4);
        ds.setConnectionInitSql("SET search_path = ag_catalog, \"$user\", public");
        try (var conn = ds.getConnection(); var st = conn.createStatement()) {
            st.execute("CREATE EXTENSION IF NOT EXISTS age");
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException("Could not create the AGE extension", e);
        }
        return ds;
    }
}
