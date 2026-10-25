package inno.ipr.ai_incident_responder_service.config;

import com.anthropic.errors.BadRequestException;
import com.anthropic.errors.NotFoundException;
import com.anthropic.errors.PermissionDeniedException;
import com.anthropic.errors.UnauthorizedException;
import com.anthropic.errors.UnprocessableEntityException;
import inno.ipr.ai_incident_responder_service.exception.UnplannableAlertException;
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
 * {@code /actuator/prometheus}); an open breaker is otherwise invisible from outside the pod. The
 * {@code circuitBreakerMetrics} bean is a {@link MeterBinder}, and Boot binds every such bean to
 * the autoconfigured {@code MeterRegistry}.
 *
 * <p>{@link #PERMANENT_FAILURES} are the failures no retry can fix: {@link UnplannableAlertException}
 * (refusal, {@code max_tokens}, no structured content) and the Anthropic 400/401/403/404/422
 * errors (a bad request, key, model id or effort value). The breaker ignores them — the API
 * answered, so they say nothing about its health and must not open the circuit for every other
 * alert — and {@code KafkaConfig} dead-letters them instead of retrying. 429, 5xx, I/O errors and an
 * open breaker stay transient.
 */
@Configuration
public class ResilienceConfig {

    public static final String CLAUDE_BREAKER = "claude";

    @SuppressWarnings("unchecked")
    public static final Class<? extends Exception>[] PERMANENT_FAILURES = new Class[] {
            UnplannableAlertException.class,
            BadRequestException.class,
            UnauthorizedException.class,
            PermissionDeniedException.class,
            NotFoundException.class,
            UnprocessableEntityException.class
    };

    @Bean
    CircuitBreakerRegistry circuitBreakerRegistry() {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(5)
                .minimumNumberOfCalls(3)
                .failureRateThreshold(50.0f)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(2)
                .ignoreExceptions(PERMANENT_FAILURES)
                .build();
        return CircuitBreakerRegistry.of(config);
    }

    @Bean
    CircuitBreaker claudeCircuitBreaker(CircuitBreakerRegistry registry) {
        return registry.circuitBreaker(CLAUDE_BREAKER);
    }

    @Bean
    MeterBinder circuitBreakerMetrics(CircuitBreakerRegistry registry) {
        return TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(registry);
    }
}
