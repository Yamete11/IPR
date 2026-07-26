package inno.ipr.ai_incident_responder_service.domain;

import java.time.Instant;

/**
 * A compact summary of a sensor's last hour of readings from TimescaleDB — the time-series half of
 * the lightweight RAG context. Summarising (rather than dumping every reading) keeps the Claude
 * prompt within token limits. When the sensor has no readings in the window, {@link #count} is 0 and
 * the numeric fields are {@code null}.
 *
 * @param count    number of readings in the last hour
 * @param min      minimum value
 * @param max      maximum value
 * @param avg      mean value
 * @param latest   most recent value
 * @param latestAt timestamp of the most recent reading
 */
public record MetricSummary(
        long count,
        Double min,
        Double max,
        Double avg,
        Double latest,
        Instant latestAt) {

    public boolean isEmpty() {
        return count == 0;
    }
}
