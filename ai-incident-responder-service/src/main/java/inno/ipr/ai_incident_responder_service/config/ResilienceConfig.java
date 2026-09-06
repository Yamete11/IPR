package inno.ipr.ai_incident_responder_service.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * The Circuit Breaker guarding the Claude API call — the resilience pattern the design commits to.
 * Uses Resilience4j <em>core</em> (constructed here as beans) rather than the Spring-Boot-3
 * starter, which is not Boot-4 compatible.
 *
 * <p>After the failure rate crosses the threshold over the sliding window the breaker opens and
 * calls fail fast with {@code CallNotPermittedException} instead of hammering a down API; it probes
 * again after the wait duration. The consumer treats an open circuit as a transient failure, so the
 * Kafka container retries the alert later rather than dead-lettering a valid one.
 *
 * <p>The breaker lives in a {@link CircuitBreakerRegistry} so its state, call counts and failure
 * rate are published to Micrometer ({@code resilience4j_circuitbreaker_*} on
 * {@code /actuator/prometheus}); an open breaker is otherwise invisible from outside the pod.
 */
@Configuration
public class ResilienceConfig {

    public static final String CLAUDE_BREAKER = "claude";

    @Bean
    CircuitBreakerRegistry circuitBreakerRegistry() {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(5)
                .minimumNumberOfCalls(3)
                .failureRateThreshold(50.0f)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(2)
                .build();
        return CircuitBreakerRegistry.of(config);
    }

    @Bean
    CircuitBreaker claudeCircuitBreaker(CircuitBreakerRegistry registry) {
        return registry.circuitBreaker(CLAUDE_BREAKER);
    }

    /** Boot binds every {@link MeterBinder} bean to the autoconfigured {@code MeterRegistry}. */
    @Bean
    MeterBinder circuitBreakerMetrics(CircuitBreakerRegistry registry) {
        return TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(registry);
    }
}
