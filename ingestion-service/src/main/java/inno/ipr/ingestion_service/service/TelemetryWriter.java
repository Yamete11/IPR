package inno.ipr.ingestion_service.service;

import inno.ipr.ingestion_service.exception.RejectedReadingException;
import inno.ipr.ingestion_service.repository.MetricRepository;
import inno.ipr.ingestion_service.telemetry.TelemetryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

/**
 * Writes a batch of readings into the {@code metrics} hypertable. Knows nothing about Kafka.
 *
 * <p>{@link #write(List)} inserts the whole list in one JDBC batch — one round trip instead of one
 * per reading. If the database refuses the batch ({@link DataIntegrityViolationException}), it does
 * not say which row was at fault, so {@code writeOneByOne} re-inserts the readings individually to
 * find it and throws {@link RejectedReadingException} with its index. Re-inserting rows that already
 * went in is harmless: the insert is {@code ON CONFLICT DO NOTHING}. If every row goes in on its own,
 * the rejection was a property of the batch, not of any reading, and nothing is rejected.
 *
 * <p>Any other failure — a database outage above all — propagates unchanged, so the Kafka error
 * handler retries the batch.
 *
 * <p>{@code warnAboutDuplicates} counts rows the database reported as not inserted (count 0). That
 * is {@code ON CONFLICT DO NOTHING} swallowing a duplicate: expected for a Kafka redelivery, but it
 * also fires when a producer emits colliding {@code (timestamp, sensor_id, metric_type)} tuples — a
 * silent under-count — so it is logged as a warning. With {@code reWriteBatchedInserts} the driver
 * usually reports {@code SUCCESS_NO_INFO} (-2) instead of per-row counts, so this is best-effort;
 * {@code counts} is {@code null} only from a mocked repository.
 */
@Service
public class TelemetryWriter {

    private static final Logger log = LoggerFactory.getLogger(TelemetryWriter.class);

    private final MetricRepository metrics;

    public TelemetryWriter(MetricRepository metrics) {
        this.metrics = metrics;
    }

    public void write(List<TelemetryMessage> readings) {
        if (readings.isEmpty()) {
            return;
        }
        try {
            warnAboutDuplicates(readings.size(), metrics.saveAll(readings));
        } catch (DataIntegrityViolationException batchFailure) {
            log.warn("Batch of {} readings rejected ({}); re-inserting one by one to find the cause",
                    readings.size(), batchFailure.getMessage());
            writeOneByOne(readings);
        }
    }

    private void writeOneByOne(List<TelemetryMessage> readings) {
        for (int i = 0; i < readings.size(); i++) {
            try {
                metrics.save(readings.get(i));
            } catch (DataIntegrityViolationException rowFailure) {
                throw new RejectedReadingException(i, rowFailure);
            }
        }
        log.warn("All {} readings inserted one by one after the batch was rejected", readings.size());
    }

    private static void warnAboutDuplicates(int batchSize, int[] counts) {
        if (counts == null) {
            return;
        }
        long dropped = Arrays.stream(counts).filter(count -> count == 0).count();
        if (dropped > 0) {
            log.warn("Batch of {} readings: {} duplicates dropped", batchSize, dropped);
        }
    }
}
