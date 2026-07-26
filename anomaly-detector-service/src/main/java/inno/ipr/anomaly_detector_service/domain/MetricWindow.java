package inno.ipr.anomaly_detector_service.domain;

import java.time.Instant;

/**
 * One 5-minute aggregate bucket read from {@code metrics_5m_avg} for a single
 * {@code (sensorId, metricType)}. Detection rules evaluate {@link #min} / {@link #max} — the
 * boundary values that reveal a transient fault an average would hide.
 *
 * @param sensorId   producing sensor, e.g. {@code s-00042}
 * @param metricType the measured quantity, e.g. {@code pipe_pressure}
 * @param bucket     start of the 5-minute time bucket
 * @param avg        mean value in the bucket
 * @param min        minimum value in the bucket
 * @param max        maximum value in the bucket
 * @param count      number of readings aggregated
 */
public record MetricWindow(
        String sensorId,
        String metricType,
        Instant bucket,
        double avg,
        double min,
        double max,
        long count) {
}
