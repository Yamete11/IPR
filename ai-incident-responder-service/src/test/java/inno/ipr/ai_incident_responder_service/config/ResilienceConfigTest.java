package inno.ipr.ai_incident_responder_service.config;

import inno.ipr.ai_incident_responder_service.exception.UnplannableAlertException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The breaker's tuning as a behaviour: how many failures open it, and what an open breaker does.
 *
 * <p>{@code opensAfterThreeFailuresAndFailsFast}: the fourth call never reaches the (would-be) API.
 *
 * <p>{@code permanentFailuresDoNotOpenTheBreaker}: a refusal says nothing about the API's health,
 * so a run of them leaves the circuit closed for every other alert.
 */
class ResilienceConfigTest {

    private final ResilienceConfig config = new ResilienceConfig();
    private final CircuitBreakerRegistry registry = config.circuitBreakerRegistry();
    private final CircuitBreaker breaker = config.claudeCircuitBreaker(registry);

    private static final Supplier<String> FAILING = () -> {
        throw new IllegalStateException("Claude API down");
    };

    @Test
    @DisplayName("opens after the minimum number of calls have all failed, then fails fast")
    void opensAfterThreeFailuresAndFailsFast() {
        Supplier<String> guarded = CircuitBreaker.decorateSupplier(breaker, FAILING);

        for (int i = 0; i < 3; i++) {
            assertThatThrownBy(guarded::get).isInstanceOf(IllegalStateException.class);
        }
        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        assertThatThrownBy(guarded::get).isInstanceOf(CallNotPermittedException.class);
    }

    @Test
    @DisplayName("stays closed while the failure rate is under half of the window")
    void staysClosedUnderThreshold() {
        Supplier<String> ok = CircuitBreaker.decorateSupplier(breaker, () -> "plan");
        Supplier<String> failing = CircuitBreaker.decorateSupplier(breaker, FAILING);

        ok.get();
        ok.get();
        ok.get();
        assertThatThrownBy(failing::get).isInstanceOf(IllegalStateException.class);
        ok.get();

        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    @DisplayName("permanent failures pass through without counting against the breaker")
    void permanentFailuresDoNotOpenTheBreaker() {
        Supplier<String> unplannable = CircuitBreaker.decorateSupplier(breaker, () -> {
            throw new UnplannableAlertException("refusal");
        });

        for (int i = 0; i < 5; i++) {
            assertThatThrownBy(unplannable::get).isInstanceOf(UnplannableAlertException.class);
        }
        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(breaker.getMetrics().getNumberOfFailedCalls()).isZero();
    }

    @Test
    @DisplayName("is registered under the name the metrics are tagged with and binds to Micrometer")
    void registeredAndMeasured() {
        assertThat(breaker.getName()).isEqualTo(ResilienceConfig.CLAUDE_BREAKER);
        assertThat(registry.find(ResilienceConfig.CLAUDE_BREAKER)).isPresent();

        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        config.circuitBreakerMetrics(registry).bindTo(meters);
        assertThat(meters.find("resilience4j.circuitbreaker.state").tag("name", ResilienceConfig.CLAUDE_BREAKER).gauges())
                .isNotEmpty();
    }
}
