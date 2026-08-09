package inno.ipr.ingestion_service.telemetry;

import inno.ipr.ingestion_service.exception.InvalidTelemetryException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.json.JsonTest;
import org.springframework.context.annotation.Import;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Parser/validation contract for {@code city-telemetry} payloads.
 *
 * <p>Deliberately a {@link JsonTest} slice rather than a plain unit test with a hand-rolled
 * {@code ObjectMapper}: several behaviours here depend on Boot's Jackson configuration rather than
 * Jackson's defaults — most notably that unknown properties are ignored, which is what turns a
 * producer's misspelled {@code value} field into a missing one. A locally constructed mapper would
 * reject that payload as malformed instead and the test would assert a behaviour production does not
 * have.
 */
@JsonTest
@Import(TelemetryParser.class)
class TelemetryParserTest {

    private static final String TS = "2026-07-24T12:00:00Z";

    @Autowired
    private TelemetryParser parser;

    private static String json(String body) {
        return "{" + body + "}";
    }

    @Nested
    @DisplayName("valid payloads")
    class Valid {

        @Test
        void bindsEveryFieldFromSnakeCaseJson() {
            TelemetryMessage m = parser.parse(json("""
                    "timestamp":"%s","sensor_id":"s-00042","metric_type":"pipe_pressure",
                    "value":4.25,"status":"OK"
                    """.formatted(TS)));

            assertThat(m.timestamp()).isEqualTo(Instant.parse(TS));
            assertThat(m.sensorId()).isEqualTo("s-00042");
            assertThat(m.metricType()).isEqualTo("pipe_pressure");
            assertThat(m.value()).isEqualTo(4.25);
            assertThat(m.status()).isEqualTo("OK");
        }

        /**
         * The point of the missing-value checks is that a fabricated 0.0 must not reach the
         * hypertable — not that zero is invalid. A sensor genuinely reading zero (a dead line, a
         * drained pipe) is exactly the reading the anomaly detector needs to see.
         */
        @Test
        void acceptsAGenuineZeroReading() {
            TelemetryMessage m = parser.parse(json("""
                    "timestamp":"%s","sensor_id":"s-00042","metric_type":"pipe_pressure",
                    "value":0.0,"status":"CRITICAL"
                    """.formatted(TS)));

            assertThat(m.value()).isZero();
        }

        @Test
        void acceptsNegativeAndExtremeFiniteValues() {
            assertThatCode(() -> parser.parse(json("""
                    "timestamp":"%s","sensor_id":"s-1","metric_type":"temperature",
                    "value":-40.5,"status":"OK"
                    """.formatted(TS)))).doesNotThrowAnyException();

            assertThatCode(() -> parser.parse(json("""
                    "timestamp":"%s","sensor_id":"s-1","metric_type":"temperature",
                    "value":1.7976931348623157E308,"status":"OK"
                    """.formatted(TS)))).doesNotThrowAnyException();
        }

        @Test
        void ignoresPropertiesTheProducerAddsLater() {
            assertThatCode(() -> parser.parse(json("""
                    "timestamp":"%s","sensor_id":"s-1","metric_type":"pipe_pressure",
                    "value":1.0,"status":"OK","firmware":"2.1.0"
                    """.formatted(TS)))).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("'value' — the field whose absence used to be ingested as 0.0")
    class ValueValidation {

        @Test
        void rejectsAnOmittedValue() {
            assertThatExceptionOfType(InvalidTelemetryException.class)
                    .isThrownBy(() -> parser.parse(json("""
                            "timestamp":"%s","sensor_id":"s-1","metric_type":"substation_voltage",
                            "status":"OK"
                            """.formatted(TS))))
                    .withMessage("Missing 'value'");
        }

        @Test
        void rejectsAnExplicitNullValue() {
            assertThatExceptionOfType(InvalidTelemetryException.class)
                    .isThrownBy(() -> parser.parse(json("""
                            "timestamp":"%s","sensor_id":"s-1","metric_type":"substation_voltage",
                            "value":null,"status":"OK"
                            """.formatted(TS))))
                    .withMessage("Missing 'value'");
        }

        /**
         * A producer-side typo is the realistic form of this bug: the payload is well-formed, every
         * other field is right, and Boot's mapper drops the unknown property silently.
         */
        @Test
        void rejectsAMisspelledValueField() {
            assertThatExceptionOfType(InvalidTelemetryException.class)
                    .isThrownBy(() -> parser.parse(json("""
                            "timestamp":"%s","sensor_id":"s-1","metric_type":"substation_voltage",
                            "valeu":220.0,"status":"OK"
                            """.formatted(TS))))
                    .withMessage("Missing 'value'");
        }

        @Test
        void rejectsValuesThatOverflowToInfinity() {
            assertThatExceptionOfType(InvalidTelemetryException.class)
                    .isThrownBy(() -> parser.parse(json("""
                            "timestamp":"%s","sensor_id":"s-1","metric_type":"pipe_pressure",
                            "value":1e400,"status":"OK"
                            """.formatted(TS))))
                    .withMessageContaining("Non-finite 'value'")
                    .withMessageContaining("Infinity");

            assertThatExceptionOfType(InvalidTelemetryException.class)
                    .isThrownBy(() -> parser.parse(json("""
                            "timestamp":"%s","sensor_id":"s-1","metric_type":"pipe_pressure",
                            "value":-1e400,"status":"OK"
                            """.formatted(TS))))
                    .withMessageContaining("Non-finite 'value'");
        }
    }

    @Nested
    @DisplayName("required identity and status fields")
    class RequiredFields {

        @Test
        void rejectsAnOmittedTimestamp() {
            assertThatExceptionOfType(InvalidTelemetryException.class)
                    .isThrownBy(() -> parser.parse(json("""
                            "sensor_id":"s-1","metric_type":"pipe_pressure","value":1.0,"status":"OK"
                            """)))
                    .withMessage("Missing 'timestamp'");
        }

        @Test
        void rejectsAnOmittedSensorId() {
            assertThatExceptionOfType(InvalidTelemetryException.class)
                    .isThrownBy(() -> parser.parse(json("""
                            "timestamp":"%s","metric_type":"pipe_pressure","value":1.0,"status":"OK"
                            """.formatted(TS))))
                    .withMessage("Missing 'sensor_id'");
        }

        @Test
        void rejectsABlankSensorId() {
            assertThatExceptionOfType(InvalidTelemetryException.class)
                    .isThrownBy(() -> parser.parse(json("""
                            "timestamp":"%s","sensor_id":"   ","metric_type":"pipe_pressure",
                            "value":1.0,"status":"OK"
                            """.formatted(TS))))
                    .withMessage("Missing 'sensor_id'");
        }

        @Test
        void rejectsAnOmittedMetricType() {
            assertThatExceptionOfType(InvalidTelemetryException.class)
                    .isThrownBy(() -> parser.parse(json("""
                            "timestamp":"%s","sensor_id":"s-1","value":1.0,"status":"OK"
                            """.formatted(TS))))
                    .withMessage("Missing 'metric_type'");
        }

        @Test
        void rejectsAnOmittedStatus() {
            assertThatExceptionOfType(InvalidTelemetryException.class)
                    .isThrownBy(() -> parser.parse(json("""
                            "timestamp":"%s","sensor_id":"s-1","metric_type":"pipe_pressure",
                            "value":1.0
                            """.formatted(TS))))
                    .withMessage("Missing 'status'");
        }
    }

    @Nested
    @DisplayName("malformed input")
    class Malformed {

        @Test
        void rejectsTruncatedJson() {
            assertThatExceptionOfType(InvalidTelemetryException.class)
                    .isThrownBy(() -> parser.parse("{\"sensor_id\":\"s-1\""))
                    .withMessage("Malformed telemetry JSON");
        }

        @Test
        void rejectsAValueThatIsNotANumber() {
            assertThatExceptionOfType(InvalidTelemetryException.class)
                    .isThrownBy(() -> parser.parse(json("""
                            "timestamp":"%s","sensor_id":"s-1","metric_type":"pipe_pressure",
                            "value":"not-a-number","status":"OK"
                            """.formatted(TS))))
                    .withMessage("Malformed telemetry JSON");
        }

        @Test
        void rejectsAnUnparseableTimestamp() {
            assertThatExceptionOfType(InvalidTelemetryException.class)
                    .isThrownBy(() -> parser.parse(json("""
                            "timestamp":"yesterday","sensor_id":"s-1","metric_type":"pipe_pressure",
                            "value":1.0,"status":"OK"
                            """)))
                    .withMessage("Malformed telemetry JSON");
        }

        @Test
        void rejectsAJsonArray() {
            assertThatExceptionOfType(InvalidTelemetryException.class)
                    .isThrownBy(() -> parser.parse("[]"))
                    .withMessage("Malformed telemetry JSON");
        }
    }

    /**
     * The DLQ recoverer in {@code KafkaConfig} logs the payload it pulls back off the exception, so
     * an empty or lossy payload would leave operators debugging a dead-lettered record blind.
     */
    @Nested
    @DisplayName("payload retained for dead-letter logging")
    class PayloadRetention {

        @Test
        void keepsTheRawPayloadOnAValidationFailure() {
            String raw = json("""
                    "timestamp":"%s","sensor_id":"s-1","metric_type":"pipe_pressure","status":"OK"
                    """.formatted(TS));

            assertThatExceptionOfType(InvalidTelemetryException.class)
                    .isThrownBy(() -> parser.parse(raw))
                    .satisfies(e -> assertThat(e.payload()).isEqualTo(raw));
        }

        @Test
        void keepsTheRawPayloadOnAParseFailure() {
            String raw = "{\"sensor_id\":\"s-1\"";

            assertThatExceptionOfType(InvalidTelemetryException.class)
                    .isThrownBy(() -> parser.parse(raw))
                    .satisfies(e -> {
                        assertThat(e.payload()).isEqualTo(raw);
                        assertThat(e.getCause()).isNotNull();
                    });
        }
    }
}
