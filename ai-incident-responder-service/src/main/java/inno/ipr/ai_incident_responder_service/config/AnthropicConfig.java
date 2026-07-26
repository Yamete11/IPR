package inno.ipr.ai_incident_responder_service.config;

import com.anthropic.client.AnthropicClient;
import com.anthropic.client.okhttp.AnthropicOkHttpClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The Claude API client. {@link AnthropicOkHttpClient#fromEnv()} reads {@code ANTHROPIC_API_KEY}
 * from the environment (a K8s Secret in production); the app fails fast at startup if it is absent,
 * which is the desired signal for a service whose whole job is calling Claude.
 *
 * <p>Not created when {@code ai.claude.mock=true} — the {@link com.anthropic.client.AnthropicClient}
 * would demand a key that offline/mock runs don't have, and {@code MockIncidentPlanner} doesn't need
 * it.
 */
@Configuration
public class AnthropicConfig {

    @Bean
    @ConditionalOnProperty(name = "ai.claude.mock", havingValue = "false", matchIfMissing = true)
    AnthropicClient anthropicClient() {
        return AnthropicOkHttpClient.fromEnv();
    }
}
