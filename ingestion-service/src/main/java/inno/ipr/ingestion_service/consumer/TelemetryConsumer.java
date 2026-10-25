package inno.ipr.ingestion_service.consumer;

import inno.ipr.ingestion_service.exception.InvalidTelemetryException;
import inno.ipr.ingestion_service.exception.RejectedReadingException;
import inno.ipr.ingestion_service.service.TelemetryWriter;
import inno.ipr.ingestion_service.telemetry.TelemetryMessage;
import inno.ipr.ingestion_service.telemetry.TelemetryParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Kafka entry point: takes each poll of {@code city-telemetry}, parses it with
 * {@link TelemetryParser} and stores it with {@link TelemetryWriter}. This class only translates
 * between Kafka and those two; parsing and storage rules live in them.
 *
 * <p>The one Kafka-specific rule is how a single bad record is isolated from its batch. Throwing
 * {@link BatchListenerFailedException} with the record's index tells the container's
 * {@code DefaultErrorHandler} to commit everything before that index, send exactly that record to
 * the DLQ (see {@code KafkaConfig}) and redeliver the rest in the next poll.
 *
 * <p>{@link #onMessages(List)} parses records in order and stops at the first one the parser
 * rejects. The readings parsed before it are stored first, so they are committed rather than
 * replayed; the ones after it are not stored at all, so their redelivery is a first insert, not a
 * duplicate. If every record parses, the whole batch is stored.
 *
 * <p>{@code store} turns the writer's {@link RejectedReadingException} into the Kafka exception. Its
 * index can be used as a record index directly because the readings are always a prefix of
 * {@code records}: the reading at position {@code i} came from the record at position {@code i}.
 * Any other failure (the database being down) propagates as-is and the error handler retries the
 * batch.
 */
@Component
public class TelemetryConsumer {

    private final TelemetryParser parser;
    private final TelemetryWriter writer;

    public TelemetryConsumer(TelemetryParser parser, TelemetryWriter writer) {
        this.parser = parser;
        this.writer = writer;
    }

    @KafkaListener(topics = "city-telemetry", groupId = "ingestion-service")
    public void onMessages(List<ConsumerRecord<String, String>> records) {
        List<TelemetryMessage> readings = new ArrayList<>(records.size());
        for (int i = 0; i < records.size(); i++) {
            try {
                readings.add(parser.parse(records.get(i).value()));
            } catch (InvalidTelemetryException poison) {
                store(readings);
                throw new BatchListenerFailedException(
                        "Poison telemetry record at batch index " + i + ": " + poison.getMessage(), poison, i);
            }
        }
        store(readings);
    }

    private void store(List<TelemetryMessage> readings) {
        try {
            writer.write(readings);
        } catch (RejectedReadingException rejected) {
            throw new BatchListenerFailedException(rejected.getMessage(), rejected.getCause(), rejected.index());
        }
    }
}
