package inno.ipr.ai_incident_responder_service.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * The Circuit Breaker guarding the Claude API call — the resilience pattern the design commits to.
 * Uses Resilience4j <em>core</em> (constructed here as a bean) rather than the Spring-Boot-3
 * starter, which is not Boot-4 compatible.
 *
 * <p>After the failure rate crosses the threshold over the sliding window the breaker opens and
 * calls fail fast with {@code CallNotPermittedException} instead of hammering a down API; it probes
 * again after the wait duration. The consumer treats an open circuit as a transient failure, so the
 * Kafka container retries the alert later rather than dead-lettering a valid one.
 */
@Configuration
public class ResilienceConfig {

    @Bean
    CircuitBreaker claudeCircuitBreaker() {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(5)
                .minimumNumberOfCalls(3)
                .failureRateThreshold(50.0f)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(2)
                .build();
        return CircuitBreaker.of("claude", config);
    }
}
