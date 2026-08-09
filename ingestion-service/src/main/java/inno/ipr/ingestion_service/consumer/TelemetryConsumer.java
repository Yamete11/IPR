package inno.ipr.ingestion_service.consumer;

import inno.ipr.ingestion_service.repository.MetricRepository;
import inno.ipr.ingestion_service.telemetry.TelemetryMessage;
import inno.ipr.ingestion_service.telemetry.TelemetryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes the {@code city-telemetry} topic and persists each reading into the {@code metrics}
 * hypertable. One record → parse/validate → insert; nothing else (this service is metrics-only and
 * never touches the AGE topology graph).
 *
 * <p>The payload arrives as a {@code String} (plain {@code StringDeserializer}); parsing and
 * validation live in {@link TelemetryParser}. Any failure throws out of this method, where the
 * container's error handler retries a couple of times and then routes the record to
 * {@code city-telemetry.DLQ} (see {@code KafkaConfig}).
 */
@Component
public class TelemetryConsumer {

    private static final Logger log = LoggerFactory.getLogger(TelemetryConsumer.class);

    private final TelemetryParser parser;
    private final MetricRepository metrics;

    public TelemetryConsumer(TelemetryParser parser, MetricRepository metrics) {
        this.parser = parser;
        this.metrics = metrics;
    }

    @KafkaListener(topics = "city-telemetry", groupId = "ingestion-service")
    public void onMessage(String payload) {
        TelemetryMessage message = parser.parse(payload);
        int stored = metrics.save(message);
        if (stored == 0) {
            // ON CONFLICT DO NOTHING swallowed the row. Expected for a Kafka redelivery, but it also
            // fires when a producer emits colliding (timestamp, sensor_id, metric_type) tuples — a
            // real under-count with no error anywhere, so it must not be logged as a success.
            log.warn("Duplicate metric dropped: sensor={} metric={} at {}",
                    message.sensorId(), message.metricType(), message.timestamp());
            return;
        }
        log.debug("Ingested metric: sensor={} metric={} value={} at {}",
                message.sensorId(), message.metricType(), message.value(), message.timestamp());
    }
}
