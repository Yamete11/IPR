package inno.ipr.ai_incident_responder_service.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * Two datasources — this is the only service that reads <em>both</em> PostgreSQL instances:
 *
 * <ul>
 *   <li><b>TimescaleDB</b> ({@code spring.datasource.timescale}) — the metric history it enriches
 *       from and the {@code incidents} table it writes. Marked {@link Primary} so Spring Boot's
 *       {@code spring.sql.init} (schema.sql) and any single-datasource machinery target it.</li>
 *   <li><b>age-db</b> ({@code spring.datasource.age}) — the AGE topology graph, read for the
 *       dependency (impacted-objects) walk. Its Hikari {@code connection-init-sql} puts
 *       {@code ag_catalog} on the search_path so {@code cypher()} resolves, mirroring
 *       topology-service.</li>
 * </ul>
 *
 * <p>Binding {@code @ConfigurationProperties} straight onto {@link HikariDataSource} keeps the two
 * pools independent; each {@link JdbcTemplate} is qualified so repositories inject the right one.
 * Defining these beans makes Boot's single-datasource autoconfiguration back off.
 */
@Configuration
public class DataSourceConfig {

    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource.timescale")
    HikariDataSource timescaleDataSource() {
        return new HikariDataSource();
    }

    @Bean
    @ConfigurationProperties("spring.datasource.age")
    HikariDataSource ageDataSource() {
        return new HikariDataSource();
    }

    @Bean
    @Primary
    JdbcTemplate timescaleJdbc(@Qualifier("timescaleDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean
    JdbcTemplate ageJdbc(@Qualifier("ageDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
