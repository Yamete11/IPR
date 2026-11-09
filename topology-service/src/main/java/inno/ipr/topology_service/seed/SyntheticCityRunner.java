package inno.ipr.topology_service.seed;

import inno.ipr.topology_service.age.AgeClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;

/**
 * Startup hook that generates the {@link SyntheticCity} for {@code topology.synthetic.sensors}
 * sensors and hands it to {@link SyntheticCityLoader}. Gated by {@code topology.synthetic.enabled}.
 *
 * <p>{@link #run(ApplicationArguments)} runs after {@link SeedLoader} ({@code @Order(2)} against its
 * {@code @Order(1)}), so the log reads hand seed first, city second. The two are independent: the
 * loader ensures its own graph and indexes. The sensor count defaults to {@code CITY_EXTRA_SENSORS},
 * the same variable the simulator reads for {@code --extra-sensors}, so both describe one city.
 */
@Component
@Order(2)
@ConditionalOnProperty(name = "topology.synthetic.enabled", havingValue = "true")
public class SyntheticCityRunner implements ApplicationRunner {

    private final SyntheticCityLoader loader;
    private final int sensors;

    public SyntheticCityRunner(AgeClient age,
                               JdbcTemplate jdbc,
                               PlatformTransactionManager transactionManager,
                               ObjectMapper objectMapper,
                               @Value("${topology.graph:city_topology}") String graph,
                               @Value("${topology.synthetic.sensors:997}") int sensors) {
        this.loader = new SyntheticCityLoader(age, jdbc, new TransactionTemplate(transactionManager),
                objectMapper, graph);
        this.sensors = sensors;
    }

    @Override
    public void run(ApplicationArguments args) {
        loader.load(SyntheticCity.generate(sensors));
    }
}
