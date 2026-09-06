package inno.ipr.ingestion_service.consumer;

import inno.ipr.ingestion_service.exception.InvalidTelemetryException;
import inno.ipr.ingestion_service.repository.MetricRepository;
import inno.ipr.ingestion_service.telemetry.TelemetryMessage;
import inno.ipr.ingestion_service.telemetry.TelemetryParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Consumes the {@code city-telemetry} topic and persists each poll's readings into the
 * {@code metrics} hypertable in one JDBC batch. Nothing else (this service is metrics-only and
 * never touches the AGE topology graph).
 *
 * <p>Batching is where the throughput lives: one round trip per poll instead of one per reading.
 * The price is that a single bad record must not take its neighbours down with it, which is what
 * {@link BatchListenerFailedException} is for. Throwing it with the offending index tells the
 * container's {@code DefaultErrorHandler} to commit everything before that index, hand exactly that
 * record to the recoverer (the DLQ — see {@code KafkaConfig}), and re-seek the rest for the next
 * poll. The valid prefix is written first, so it is committed rather than replayed; the suffix is
 * not written at all, so its replay is a first insert, not a duplicate.
 *
 * <p>Two failure classes reach that path:
 * <ul>
 *   <li><b>Poison payloads</b> ({@link InvalidTelemetryException}) are found while parsing, before
 *       any write, so the prefix is persisted and the poison record dead-lettered.</li>
 *   <li><b>Rows the database refuses</b> ({@link DataIntegrityViolationException}) surface only for
 *       the batch as a whole; the consumer then re-inserts row by row to locate the culprit (the
 *       idempotent {@code ON CONFLICT} makes the re-insert of the good rows harmless).</li>
 * </ul>
 * Any other failure — a database outage above all — propagates as-is and the error handler retries
 * the whole batch under its unbounded back-off, again safe because the insert is idempotent.
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
    public void onMessages(List<ConsumerRecord<String, String>> records) {
        List<TelemetryMessage> parsed = new ArrayList<>(records.size());
        int poisonIndex = -1;
        InvalidTelemetryException poison = null;

        for (int i = 0; i < records.size(); i++) {
            try {
                parsed.add(parse(records.get(i)));
            } catch (InvalidTelemetryException e) {
                poisonIndex = i;
                poison = e;
                break; // the records after it are re-delivered once this one is dead-lettered
            }
        }

        persist(parsed);

        if (poison != null) {
            throw new BatchListenerFailedException(
                    "Poison telemetry record at batch index " + poisonIndex + ": " + poison.getMessage(),
                    poison, poisonIndex);
        }
    }

    private TelemetryMessage parse(ConsumerRecord<String, String> record) {
        String payload = record.value();
        if (payload == null) {
            // A tombstone carries no reading; feeding null to the parser would surface as one of our
            // own exceptions and retry forever instead of being isolated as the bad data it is.
            throw new InvalidTelemetryException("Null payload (tombstone)", "");
        }
        return parser.parse(payload);
    }

    /**
     * Writes the parsed prefix in one batch. On a {@link DataIntegrityViolationException} for the
     * batch, falls back to row-by-row inserts to find the row the database rejects, and reports it
     * with its index so only that row is dead-lettered.
     */
    private void persist(List<TelemetryMessage> messages) {
        if (messages.isEmpty()) {
            return;
        }
        try {
            logOutcome(messages, metrics.saveAll(messages));
        } catch (DataIntegrityViolationException batchFailure) {
            log.warn("Batch of {} readings rejected ({}); re-inserting row by row to isolate the cause",
                    messages.size(), batchFailure.getMessage());
            for (int j = 0; j < messages.size(); j++) {
                try {
                    metrics.save(messages.get(j));
                } catch (DataIntegrityViolationException rowFailure) {
                    throw new BatchListenerFailedException(
                            "Unsatisfiable telemetry row at batch index " + j + ": " + rowFailure.getMessage(),
                            rowFailure, j);
                }
            }
            // Every row went in on its own: the rejection was a property of the batch, not of any
            // reading. Nothing to dead-letter; the data is stored.
            log.warn("All {} readings inserted individually after the batch was rejected", messages.size());
        }
    }

    private static void logOutcome(List<TelemetryMessage> messages, int[] counts) {
        if (counts == null) {
            return;
        }
        int stored = 0;
        int dropped = 0;
        int unattributed = 0;
        for (int count : counts) {
            if (count > 0) {
                stored++;
            } else if (count == 0) {
                dropped++;
            } else {
                unattributed++; // Statement.SUCCESS_NO_INFO under reWriteBatchedInserts
            }
        }
        if (dropped > 0) {
            // ON CONFLICT DO NOTHING swallowed rows. Expected for a Kafka redelivery, but it also
            // fires when a producer emits colliding (timestamp, sensor_id, metric_type) tuples — a
            // real under-count with no error anywhere, so it must not be logged as a success.
            log.warn("Batch of {} readings: {} stored, {} duplicates dropped", messages.size(), stored, dropped);
        } else if (log.isDebugEnabled()) {
            log.debug("Batch of {} readings persisted ({} counted, {} unattributed)",
                    messages.size(), stored, unattributed);
        }
    }
}
