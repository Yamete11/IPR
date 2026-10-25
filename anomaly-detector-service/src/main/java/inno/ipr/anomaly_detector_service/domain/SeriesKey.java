package inno.ipr.anomaly_detector_service.domain;

/**
 * Identifies one monitored time series: a single metric of a single sensor. The detector groups
 * windows and tracks alert state per series.
 *
 * @param sensorId   producing sensor, e.g. {@code s-00042}
 * @param metricType the measured quantity, e.g. {@code pipe_pressure}
 */
public record SeriesKey(String sensorId, String metricType) {
}
