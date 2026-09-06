package inno.ipr.anomaly_detector_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Anomaly detector — Stage 3 (intelligence layer). Polls TimescaleDB's {@code metrics_5m_avg}
 * continuous aggregate on a schedule, evaluates threshold rules on the boundary values, and
 * produces critical events to the {@code infrastructure-alerts} Kafka topic. {@link EnableScheduling}
 * activates the {@code @Scheduled} detection loop; {@link ConfigurationPropertiesScan} binds the
 * externalised threshold rules ({@code anomaly.rules[]}).
 */
@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
public class AnomalyDetectorServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(AnomalyDetectorServiceApplication.class, args);
	}

}
