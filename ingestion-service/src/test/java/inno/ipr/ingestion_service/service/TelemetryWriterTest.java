package inno.ipr.ingestion_service.service;

import inno.ipr.ingestion_service.exception.RejectedReadingException;
import inno.ipr.ingestion_service.repository.MetricRepository;
import inno.ipr.ingestion_service.telemetry.TelemetryMessage;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.QueryTimeoutException;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Storage strategy of {@link TelemetryWriter}, without Kafka or a database: the repository is a
 * Mockito mock, so each test sets up exactly the outcome it needs.
 *
 * <ul>
 *   <li>{@link #writesTheWholeListInOneBatch()}: the happy path is one {@code saveAll}, no
 *       single-row inserts.</li>
 *   <li>{@link #findsTheRowTheDatabaseRejects()}: when the batch is refused, the writer re-inserts
 *       one by one and reports the index of the first row the database refuses on its own.</li>
 *   <li>{@link #rejectsNothingWhenEveryRowGoesInAlone()}: a batch-level refusal that no single row
 *       reproduces is not blamed on any reading.</li>
 *   <li>{@link #letsOtherFailuresThrough()}: an outage is not a bad reading; it must reach the Kafka
 *       error handler unchanged so the batch is retried.</li>
 * </ul>
 *
 * <p>{@code reading(String)} builds a valid reading for a sensor id; {@code rejection()} is the
 * exception a constraint violation surfaces as.
 */
class TelemetryWriterTest {

    private final MetricRepository metrics = mock(MetricRepository.class);
    private final TelemetryWriter writer = new TelemetryWriter(metrics);

    @Test
    void ignoresAnEmptyList() {
        writer.write(List.of());

        verifyNoInteractions(metrics);
    }

    @Test
    void writesTheWholeListInOneBatch() {
        List<TelemetryMessage> readings = List.of(reading("s-1"), reading("s-2"));
        given(metrics.saveAll(readings)).willReturn(new int[]{1, 1});

        writer.write(readings);

        verify(metrics).saveAll(readings);
        verify(metrics, never()).save(any());
    }

    @Test
    void findsTheRowTheDatabaseRejects() {
        TelemetryMessage good = reading("s-1");
        TelemetryMessage bad = reading("s-2");
        given(metrics.saveAll(any())).willThrow(rejection());
        given(metrics.save(bad)).willThrow(rejection());

        assertThatExceptionOfType(RejectedReadingException.class)
                .isThrownBy(() -> writer.write(List.of(good, bad, reading("s-3"))))
                .satisfies(e -> assertThat(e.index()).isEqualTo(1))
                .withCauseInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void rejectsNothingWhenEveryRowGoesInAlone() {
        given(metrics.saveAll(any())).willThrow(rejection());

        assertThatCode(() -> writer.write(List.of(reading("s-1"), reading("s-2"))))
                .doesNotThrowAnyException();
    }

    @Test
    void letsOtherFailuresThrough() {
        given(metrics.saveAll(any())).willThrow(new QueryTimeoutException("database is down"));

        assertThatExceptionOfType(QueryTimeoutException.class)
                .isThrownBy(() -> writer.write(List.of(reading("s-1"))));
        verify(metrics, never()).save(any());
    }

    private static TelemetryMessage reading(String sensorId) {
        return new TelemetryMessage(Instant.parse("2026-07-24T12:00:00Z"), sensorId, "pipe_pressure", 4.0, "OK");
    }

    private static DataIntegrityViolationException rejection() {
        return new DataIntegrityViolationException("violates check constraint");
    }
}
