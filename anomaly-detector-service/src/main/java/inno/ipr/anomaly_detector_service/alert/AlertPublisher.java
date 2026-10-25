package inno.ipr.anomaly_detector_service.alert;

import inno.ipr.anomaly_detector_service.config.KafkaConfig;
import inno.ipr.anomaly_detector_service.detection.ThresholdRule;
import inno.ipr.anomaly_detector_service.domain.SeriesKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Publishes {@link Alert}s to {@code infrastructure-alerts} with <em>edge-triggered</em> dedup: an
 * alert fires only on the OK→breach transition, so a standing fault does not re-fire on every poll
 * and flood the responder. {@code active} maps each breaching series to the id of the alert that
 * armed it. Alert ids are full UUIDs because the responder dedups on {@code alert_id}. State is
 * in-memory, so a restart re-alerts any still-active fault.
 *
 * <p>{@link #publishIfNew(SeriesKey, ThresholdRule.Breach)} arms the series and sends an alert iff it
 * was not already armed, returning {@code true} on the rising edge and {@code false} for a suppressed
 * duplicate. If the record never reaches the broker (a delivery failure, or a serialisation or send
 * failure thrown synchronously) the series is disarmed again, so the next poll retries instead of the
 * fault going unreported for as long as it stands. The disarm is conditional on the key still holding
 * <em>this</em> alert's id, so a late failure of an earlier alert cannot disarm a newer breach.
 *
 * <p>{@link #clear(SeriesKey)} marks a series healthy so its next breach fires; a no-op if it was not
 * armed.
 *
 * <p>{@link #retainOnly(Set)} disarms every armed series absent from this poll. A sensor that goes
 * silent mid-fault never produces the healthy window {@code clear} waits for, so otherwise its next
 * real fault would be suppressed.
 */
@Component
public class AlertPublisher {

    private static final Logger log = LoggerFactory.getLogger(AlertPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final ConcurrentMap<SeriesKey, String> active = new ConcurrentHashMap<>();

    public AlertPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public boolean publishIfNew(SeriesKey series, ThresholdRule.Breach breach) {
        String alertId = "a-" + UUID.randomUUID();
        if (active.putIfAbsent(series, alertId) != null) {
            return false;
        }

        Alert alert = new Alert(alertId, series.sensorId(), series.metricType(), breach.value(),
                Instant.now(), breach.rule());
        try {
            kafkaTemplate.send(KafkaConfig.ALERTS_TOPIC, series.sensorId(), objectMapper.writeValueAsString(alert))
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            active.remove(series, alertId);
                            log.error("Alert {} for {} was NOT delivered; the next poll retries it",
                                    alertId, series, ex);
                        }
                    });
        } catch (RuntimeException e) {
            active.remove(series, alertId);
            throw e;
        }

        log.warn("ALERT {} for {} (value={}, rule={})", alertId, series, breach.value(), breach.rule());
        return true;
    }

    public void clear(SeriesKey series) {
        if (active.remove(series) != null) {
            log.info("Recovered: {} back within threshold", series);
        }
    }

    public void retainOnly(Set<SeriesKey> seen) {
        active.keySet().removeIf(series -> {
            boolean gone = !seen.contains(series);
            if (gone) {
                log.info("{} stopped reporting while alerting; disarming it", series);
            }
            return gone;
        });
    }
}
