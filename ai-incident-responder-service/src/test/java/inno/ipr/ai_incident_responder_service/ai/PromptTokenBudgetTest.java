package inno.ipr.ai_incident_responder_service.ai;

import com.anthropic.client.AnthropicClient;
import com.anthropic.client.okhttp.AnthropicOkHttpClient;
import com.anthropic.models.messages.MessageCountTokensParams;
import com.anthropic.models.messages.MessageCreateParams;
import inno.ipr.ai_incident_responder_service.alert.Alert;
import inno.ipr.ai_incident_responder_service.domain.ImpactedObject;
import inno.ipr.ai_incident_responder_service.domain.MetricSummary;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures the real input-token cost of the incident prompt with the Messages count-tokens
 * endpoint — the exact request {@link ClaudeIncidentPlanner} sends (system prompt, RAG context,
 * structured-output schema, adaptive thinking), tokenized by the target model, with no generation
 * and no billed output. Shows what each part of the context costs and that the impacted-objects cap
 * keeps a city-wide outage within the same budget as a small one.
 *
 * <p>Needs {@code ANTHROPIC_API_KEY}; run with {@code ./gradlew promptTokens} (excluded from
 * {@code build}).
 *
 * <p>{@code INPUT_BUDGET} is a generous ceiling for the input side; a 1M-token context window makes
 * it a cost bound, not a fit bound.
 */
@Tag("llm")
@EnabledIfEnvironmentVariable(named = "ANTHROPIC_API_KEY", matches = ".+")
class PromptTokenBudgetTest {

    private static final long INPUT_BUDGET = 4_000;

    private static final Alert ALERT = new Alert("a-1001", "s-00042", "pipe_pressure", 0.0,
            Instant.parse("2026-09-06T10:05:00Z"), "pipe_pressure_min<0.5");
    private static final MetricSummary HISTORY =
            new MetricSummary(120, 0.0, 4.4, 3.1, 0.0, Instant.parse("2026-09-06T10:04:58Z"));

    private final AnthropicClient client = AnthropicOkHttpClient.fromEnv();
    private final ClaudeIncidentPlanner planner =
            new ClaudeIncidentPlanner(client, CircuitBreaker.ofDefaults("test"), "claude-opus-5-5", "high");

    @Test
    @DisplayName("token cost of each prompt part, and the impacted-objects cap bounds the total")
    void tokenBudget() {
        long empty = count(new MetricSummary(0, null, null, null, null, null), List.of());
        long withHistory = count(HISTORY, List.of());
        long small = count(HISTORY, buildings(3));
        long capped = count(HISTORY, buildings(IncidentPromptBuilder.MAX_IMPACTED_IN_PROMPT));
        long cityWide = count(HISTORY, buildings(2_000));

        System.out.printf("""
                prompt input tokens (claude-opus-5-5, system + schema + user message):
                  no history, no graph       %6d
                  + 1h metric summary        %6d  (+%d)
                  + 3 impacted objects       %6d  (+%d, ~%d per object)
                  + %d impacted (the cap)    %6d
                  2000 impacted (capped)     %6d
                """, empty, withHistory, withHistory - empty, small, small - withHistory,
                (small - withHistory) / 3, IncidentPromptBuilder.MAX_IMPACTED_IN_PROMPT, capped, cityWide);

        assertThat(withHistory).isGreaterThan(empty);
        assertThat(cityWide).as("the cap: 2000 objects cost about as much as 50")
                .isLessThan(capped + 50);
        assertThat(cityWide).isLessThan(INPUT_BUDGET);
    }

    private long count(MetricSummary history, List<ImpactedObject> impacted) {
        MessageCreateParams raw = planner.buildParams(ALERT, history, impacted).rawParams();
        MessageCountTokensParams params = MessageCountTokensParams.builder()
                .model(raw.model())
                .system(raw.system().orElseThrow().asString())
                .messages(raw.messages())
                .thinking(raw.thinking().orElseThrow())
                .outputConfig(raw.outputConfig().orElseThrow())
                .build();
        return client.messages().countTokens(params).inputTokens();
    }

    private static List<ImpactedObject> buildings(int n) {
        return IntStream.range(0, n)
                .mapToObj(i -> new ImpactedObject("bld-" + i, "ResidentialBuilding", Map.of("name", "Block " + i)))
                .toList();
    }
}
