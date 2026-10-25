package inno.ipr.ai_incident_responder_service.config;

import com.anthropic.errors.BadRequestException;
import com.anthropic.errors.UnauthorizedException;
import inno.ipr.ai_incident_responder_service.exception.InvalidAlertException;
import inno.ipr.ai_incident_responder_service.exception.UnplannableAlertException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * The error handler's retry classification — which failures dead-letter at once and which wait.
 * {@link DefaultErrorHandler#removeClassification(Class)} reports an explicit classification as
 * {@code false} (not retryable), so each assertion reads one entry of the table.
 *
 * <p>{@code permanentFailuresAreNotRetried}: poison alerts, Claude refusals/truncations and
 * Anthropic client errors go straight to the DLQ instead of blocking their partition forever.
 */
class KafkaConfigTest {

    @Test
    @DisplayName("poison alerts, unplannable alerts and Anthropic 4xx errors are not retried")
    @SuppressWarnings("unchecked")
    void permanentFailuresAreNotRetried() {
        DefaultErrorHandler handler = new KafkaConfig().errorHandler(mock(KafkaTemplate.class),
                new KafkaListenerEndpointRegistry(), new ThreadPoolTaskScheduler());

        assertThat(handler.removeClassification(InvalidAlertException.class)).isFalse();
        assertThat(handler.removeClassification(UnplannableAlertException.class)).isFalse();
        assertThat(handler.removeClassification(BadRequestException.class)).isFalse();
        assertThat(handler.removeClassification(UnauthorizedException.class)).isFalse();
    }
}
