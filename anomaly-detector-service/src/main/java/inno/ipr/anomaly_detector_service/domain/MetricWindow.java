package inno.ipr.anomaly_detector_service.domain;

import java.time.Instant;

/**
 * One 5-minute bucket read from {@code metrics_5m_avg} for a single series. Detection rules evaluate
 * {@link #min} / {@link #max}: the boundary values that reveal a transient fault an average would
 * hide.
 *
 * <p>{@link #series()} returns the series this window belongs to.
 *
 * @param sensorId   producing sensor, e.g. {@code s-00042}
 * @param metricType the measured quantity, e.g. {@code pipe_pressure}
 * @param bucket     start of the 5-minute time bucket
 * @param min        minimum value in the bucket
 * @param max        maximum value in the bucket
 */
public record MetricWindow(
        String sensorId,
        String metricType,
        Instant bucket,
        double min,
        double max) {

    public SeriesKey series() {
        return new SeriesKey(sensorId, metricType);
    }
}
