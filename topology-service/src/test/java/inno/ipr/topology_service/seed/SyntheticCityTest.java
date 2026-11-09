package inno.ipr.topology_service.seed;

import inno.ipr.topology_service.domain.EdgeType;
import inno.ipr.topology_service.domain.VertexLabel;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The layout rule of {@link SyntheticCity}. The fixture ids in {@code sensorsLinkToTheFixtureAssets}
 * are pinned by {@code test_sensor_simulator.py} too: the simulator's cascade relies on the same
 * mapping, so a change on either side must fail both suites.
 *
 * <p>{@code countsForTheChartProfile} pins the city for the 997 sensors the Helm chart configures.
 * {@code smallerCityIsAPrefix} shows a sensor's asset does not depend on the total, so a simulator and
 * a topology-service with different counts never disagree about where a shared sensor sits.
 */
class SyntheticCityTest {

    @Test
    void countsForTheChartProfile() {
        SyntheticCity city = SyntheticCity.generate(997);

        assertThat(rows(city, VertexLabel.SUBSTATION)).hasSize(21);
        assertThat(rows(city, VertexLabel.DISTRIBUTION_NODE)).hasSize(84);
        assertThat(rows(city, VertexLabel.RESIDENTIAL_BUILDING)).hasSize(333);
        assertThat(rows(city, VertexLabel.SENSOR)).hasSize(997);
        assertThat(city.vertexCount()).isEqualTo(1435);
        assertThat(city.edgeCount()).isEqualTo(997 + 333 + 84);
        assertThat(city.crews()).hasSize(21);
        assertThat(city.assignments()).hasSize(21 + 84 + 333);
        assertThat(city.fingerprint()).isEqualTo("synthetic-v1;n=997;b=4;k=4");
    }

    @Test
    void sensorsLinkToTheFixtureAssets() {
        SyntheticCity city = SyntheticCity.generate(997);

        assertThat(target(city, "s-01000")).isEqualTo("bld-d00-0-0");
        assertThat(target(city, "s-01001")).isEqualTo("node-d00-0");
        assertThat(target(city, "s-01002")).isEqualTo("sub-d00");
        assertThat(target(city, "s-01012")).isEqualTo("bld-d00-1-0");
        assertThat(target(city, "s-01996")).isEqualTo("bld-d20-3-0");
    }

    @Test
    void sensorsCarryTheSimulatorMetricOrder() {
        SyntheticCity city = SyntheticCity.generate(6);

        assertThat(rows(city, VertexLabel.SENSOR)).extracting(r -> r.get("metric_type")).containsExactly(
                "pipe_pressure", "cable_temperature", "substation_voltage",
                "pipe_pressure", "cable_temperature", "substation_voltage");
    }

    @Test
    void districtCrewCoversEveryAssetOfItsDistrict() {
        SyntheticCity city = SyntheticCity.generate(997);

        assertThat(city.assignments()).filteredOn(a -> a.crewId().equals("crew-d00"))
                .extracting(SyntheticCity.Assignment::assetId)
                .hasSize(1 + 4 + 16)
                .contains("sub-d00", "node-d00-3", "bld-d00-3-3");
        assertThat(city.crews().getFirst()).isEqualTo(
                new SyntheticCity.Crew("crew-d00", "District 00 crew", "+7-900-100-0000"));
    }

    @Test
    void everyVertexCarriesItsDistrict() {
        SyntheticCity city = SyntheticCity.generate(60);

        assertThat(city.vertices()).flatExtracting(SyntheticCity.VertexBatch::rows)
                .allSatisfy(row -> assertThat(row.get("district")).isNotNull());
        assertThat(rows(city, VertexLabel.RESIDENTIAL_BUILDING))
                .allSatisfy(row -> assertThat((Integer) row.get("apartments")).isBetween(40, 159));
    }

    @Test
    void smallerCityIsAPrefix() {
        SyntheticCity small = SyntheticCity.generate(60);
        SyntheticCity large = SyntheticCity.generate(997);

        for (int i = 0; i < 60; i++) {
            String sensor = SyntheticCity.sensorId(i);
            assertThat(target(small, sensor)).as(sensor).isEqualTo(target(large, sensor));
        }
    }

    @Test
    void emptyCityAndInvalidCount() {
        assertThat(SyntheticCity.generate(0).vertexCount()).isZero();
        assertThatThrownBy(() -> SyntheticCity.generate(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    private static List<Map<String, Object>> rows(SyntheticCity city, VertexLabel label) {
        return city.vertices().stream().filter(b -> b.label() == label).findFirst().orElseThrow().rows();
    }

    private static String target(SyntheticCity city, String sensorId) {
        return city.edges().stream()
                .filter(b -> b.type() == EdgeType.SENSOR_LINK)
                .flatMap(b -> b.rows().stream())
                .filter(r -> r.get("s").equals(sensorId))
                .map(r -> Objects.toString(r.get("d")))
                .findFirst().orElseThrow();
    }
}
