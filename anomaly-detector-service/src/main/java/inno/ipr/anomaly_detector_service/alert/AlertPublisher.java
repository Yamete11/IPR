package inno.ipr.anomaly_detector_service.alert;

import inno.ipr.anomaly_detector_service.config.KafkaConfig;
import inno.ipr.anomaly_detector_service.detection.ThresholdRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Publishes {@link Alert}s to {@code infrastructure-alerts} with <em>edge-triggered</em> dedup: an
 * alert fires only on the OK→breach transition, not on every poll a fault persists. A standing
 * fault would otherwise re-fire on every scheduler tick and flood the downstream responder.
 *
 * <p>The active set holds one key per breaching series ({@code sensorId|metricType}). {@code
 * publishIfNew} sends only when the key is newly added; {@code clear} removes it on recovery so the
 * next breach of that series fires again. A {@link ConcurrentHashMap}-backed set keeps this safe if
 * the scheduler ever overlaps. State is in-memory, so a restart re-alerts any still-active fault —
 * acceptable for this design.
 *
 * <p>The key is <em>disarmed again</em> if the send never reaches the broker. Marking a series
 * alerting on the strength of an enqueue alone loses the alert twice over: the record never lands on
 * the topic, and the edge trigger then believes the series is already alerting, so no later poll
 * re-fires it for as long as the fault stands. A pipe sitting at zero pressure would produce no
 * incident at all.
 */
@Component
public class AlertPublisher {

    private static final Logger log = LoggerFactory.getLogger(AlertPublisher.class);

    private final KafkaTemplate<String, String> alertKafkaTemplate;
    private final ObjectMapper objectMapper;
    private final Set<String> active = ConcurrentHashMap.newKeySet();

    public AlertPublisher(KafkaTemplate<String, String> alertKafkaTemplate, ObjectMapper objectMapper) {
        this.alertKafkaTemplate = alertKafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Sends an alert for a breaching series iff it was not already breaching. Returns {@code true}
     * when an alert was handed to the producer (the rising edge), {@code false} when the breach was
     * already active and thus suppressed.
     *
     * <p>A delivery failure (or a serialisation failure before it) removes the key again, so the
     * next scheduler tick re-evaluates the still-breaching series and retries the alert.
     */
    public boolean publishIfNew(String sensorId, String metricType, ThresholdRule.Breach breach) {
        String key = key(sensorId, metricType);
        if (!active.add(key)) {
            return false; // already breaching — suppress duplicate
        }

        Alert alert = new Alert(
                "a-" + UUID.randomUUID().toString().substring(0, 8),
                sensorId,
                metricType,
                breach.value(),
                Instant.now(),
                breach.rule());

        String json;
        try {
            json = objectMapper.writeValueAsString(alert);
        } catch (RuntimeException e) {
            // Nothing was sent, so leaving the key armed would silence this series indefinitely.
            active.remove(key);
            throw e;
        }

        alertKafkaTemplate.send(KafkaConfig.ALERTS_TOPIC, sensorId, json)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        active.remove(key);
                        log.error("Alert {} for {} on {} was NOT delivered — re-arming the edge trigger "
                                        + "so the next poll retries it",
                                alert.alertId(), metricType, sensorId, ex);
                    }
                });

        log.warn("ALERT {} — {} on {} (value={}, rule={})",
                alert.alertId(), metricType, sensorId, breach.value(), breach.rule());
        return true;
    }

    /**
     * Marks a series healthy again, so a subsequent breach re-fires. No-op if it was not active.
     */
    public void clear(String sensorId, String metricType) {
        if (active.remove(key(sensorId, metricType))) {
            log.info("Recovered — {} on {} back within threshold", metricType, sensorId);
        }
    }

    private static String key(String sensorId, String metricType) {
        return sensorId + "|" + metricType;
    }
}
