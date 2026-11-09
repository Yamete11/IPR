package inno.ipr.topology_service.seed;

import inno.ipr.topology_service.domain.EdgeType;
import inno.ipr.topology_service.domain.VertexLabel;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A deterministic city topology covering every synthetic sensor of {@code sensor-simulator}, so an
 * alert from any of them resolves to real assets, dependents and a repair crew in the graph.
 *
 * <p>The layout is a pure function of the sensor index and is mirrored by {@code layout_of} in
 * {@code sensor_simulator.py}; both sides pin the same fixture ids in their tests. Synthetic sensor
 * {@code i} is {@code s-<1000+i>} with metric {@code METRIC_ORDER[i % 3]} (the simulator's own rule).
 * Sensors come in triples {@code t = i / 3}: triple {@code t} owns building {@code t}, which hangs
 * off node {@code t / BUILDINGS_PER_NODE}, which hangs off substation
 * {@code node / NODES_PER_SUBSTATION}. The pressure sensor of a triple watches its building, the
 * cable-temperature sensor its node, the voltage sensor its substation. The two fan-outs are
 * constants, not configuration, so a simulator and a topology-service running with different sensor
 * counts disagree only on how many sensors are linked, never on where a sensor is linked.
 *
 * <p>Ids are {@code sub-dDD}, {@code node-dDD-k}, {@code bld-dDD-k-b} and {@code crew-dDD}, where
 * {@code dDD} is the district (one per substation). Every generated vertex carries
 * {@code district: "dDD"}; the loader deletes the city by that property and the dashboard filters by
 * it. {@code d}, not {@code s}, keeps these ids from reading like sensor ids.
 *
 * <p>{@link #generate(int)} builds the city for {@code sensors} synthetic sensors. The result is
 * ready for batched loading: vertex rows per label (each row's keys are that label's property
 * names), edge rows {@code {s, d}} grouped by type and endpoint labels, and one crew per district
 * assigned to every asset of its district.
 *
 * <p>{@link #fingerprint()} identifies the generated layout; the loader stores it and reloads the
 * city only when it changes. Bump {@code VERSION} when the layout rule itself changes.
 *
 * <p>{@link #vertexCount()} and {@link #edgeCount()} total the rows, for logging and tests.
 *
 * <p>{@link VertexBatch} is all vertices of one label; {@link EdgeBatch} all edges of one type
 * between one pair of labels, which lets the loader label both {@code MATCH} endpoints (an unlabeled
 * endpoint scans every label table). {@link Crew} and {@link Assignment} are rows of
 * {@code public.repair_crews} and {@code public.crew_assignments}.
 *
 * <p>{@code apartments} is {@code 40 + (37·t mod 120)}: deterministic, varied, never zero.
 */
public record SyntheticCity(
        int sensors,
        List<VertexBatch> vertices,
        List<EdgeBatch> edges,
        List<Crew> crews,
        List<Assignment> assignments) {

    public static final int BUILDINGS_PER_NODE = 4;
    public static final int NODES_PER_SUBSTATION = 4;
    public static final int FIRST_SENSOR_NUMBER = 1000;
    public static final List<String> METRIC_ORDER =
            List.of("pipe_pressure", "cable_temperature", "substation_voltage");
    static final String VERSION = "synthetic-v1";

    public record VertexBatch(VertexLabel label, List<Map<String, Object>> rows) {
    }

    public record EdgeBatch(EdgeType type, VertexLabel from, VertexLabel to, List<Map<String, Object>> rows) {
    }

    public record Crew(String crewId, String name, String phone) {
    }

    public record Assignment(String crewId, String assetId) {
    }

    public static SyntheticCity generate(int sensors) {
        if (sensors < 0) {
            throw new IllegalArgumentException("sensors must be >= 0: " + sensors);
        }
        List<Map<String, Object>> substations = new ArrayList<>();
        List<Map<String, Object>> nodes = new ArrayList<>();
        List<Map<String, Object>> buildings = new ArrayList<>();
        List<Map<String, Object>> sensorRows = new ArrayList<>();
        List<Map<String, Object>> powerLines = new ArrayList<>();
        List<Map<String, Object>> pipelines = new ArrayList<>();
        List<Map<String, Object>> toBuilding = new ArrayList<>();
        List<Map<String, Object>> toNode = new ArrayList<>();
        List<Map<String, Object>> toSubstation = new ArrayList<>();
        List<Crew> crews = new ArrayList<>();
        List<Assignment> assignments = new ArrayList<>();

        int triples = (sensors + 2) / 3;
        for (int t = 0; t < triples; t++) {
            int node = t / BUILDINGS_PER_NODE;
            int substation = node / NODES_PER_SUBSTATION;
            String district = district(substation);
            String crewId = "crew-" + district;
            if (t % (BUILDINGS_PER_NODE * NODES_PER_SUBSTATION) == 0) {
                substations.add(row("id", substationId(substation), "name", "Substation " + upper(district),
                        "district", district));
                crews.add(new Crew(crewId, "District " + district.substring(1) + " crew",
                        "+7-900-100-%04d".formatted(substation)));
                assignments.add(new Assignment(crewId, substationId(substation)));
            }
            if (t % BUILDINGS_PER_NODE == 0) {
                nodes.add(row("id", nodeId(node), "name", "Node " + upper(district) + "-" + nodeIndex(node),
                        "district", district));
                powerLines.add(row("s", substationId(substation), "d", nodeId(node)));
                assignments.add(new Assignment(crewId, nodeId(node)));
            }
            buildings.add(row("id", buildingId(t), "name",
                    "Building " + upper(district) + "-" + nodeIndex(node) + "-" + buildingIndex(t),
                    "district", district, "apartments", 40 + (37 * t) % 120));
            pipelines.add(row("s", nodeId(node), "d", buildingId(t)));
            assignments.add(new Assignment(crewId, buildingId(t)));
        }
        for (int i = 0; i < sensors; i++) {
            String sensorId = sensorId(i);
            String metric = METRIC_ORDER.get(i % 3);
            int t = i / 3;
            int node = t / BUILDINGS_PER_NODE;
            int substation = node / NODES_PER_SUBSTATION;
            sensorRows.add(row("id", sensorId, "metric_type", metric, "district", district(substation)));
            switch (i % 3) {
                case 0 -> toBuilding.add(row("s", sensorId, "d", buildingId(t)));
                case 1 -> toNode.add(row("s", sensorId, "d", nodeId(node)));
                default -> toSubstation.add(row("s", sensorId, "d", substationId(substation)));
            }
        }

        List<VertexBatch> vertices = List.of(
                new VertexBatch(VertexLabel.SUBSTATION, substations),
                new VertexBatch(VertexLabel.DISTRIBUTION_NODE, nodes),
                new VertexBatch(VertexLabel.RESIDENTIAL_BUILDING, buildings),
                new VertexBatch(VertexLabel.SENSOR, sensorRows));
        List<EdgeBatch> edges = List.of(
                new EdgeBatch(EdgeType.POWER_LINE, VertexLabel.SUBSTATION, VertexLabel.DISTRIBUTION_NODE, powerLines),
                new EdgeBatch(EdgeType.PIPELINE, VertexLabel.DISTRIBUTION_NODE, VertexLabel.RESIDENTIAL_BUILDING,
                        pipelines),
                new EdgeBatch(EdgeType.SENSOR_LINK, VertexLabel.SENSOR, VertexLabel.RESIDENTIAL_BUILDING, toBuilding),
                new EdgeBatch(EdgeType.SENSOR_LINK, VertexLabel.SENSOR, VertexLabel.DISTRIBUTION_NODE, toNode),
                new EdgeBatch(EdgeType.SENSOR_LINK, VertexLabel.SENSOR, VertexLabel.SUBSTATION, toSubstation));
        return new SyntheticCity(sensors, vertices, edges, List.copyOf(crews), List.copyOf(assignments));
    }

    public String fingerprint() {
        return VERSION + ";n=" + sensors + ";b=" + BUILDINGS_PER_NODE + ";k=" + NODES_PER_SUBSTATION;
    }

    public int vertexCount() {
        return vertices.stream().mapToInt(b -> b.rows().size()).sum();
    }

    public int edgeCount() {
        return edges.stream().mapToInt(b -> b.rows().size()).sum();
    }

    public static String sensorId(int index) {
        return "s-%05d".formatted(FIRST_SENSOR_NUMBER + index);
    }

    private static String district(int substation) {
        return "d%02d".formatted(substation);
    }

    private static String upper(String district) {
        return district.toUpperCase();
    }

    private static int nodeIndex(int node) {
        return node % NODES_PER_SUBSTATION;
    }

    private static int buildingIndex(int triple) {
        return triple % BUILDINGS_PER_NODE;
    }

    private static String substationId(int substation) {
        return "sub-" + district(substation);
    }

    private static String nodeId(int node) {
        return "node-" + district(node / NODES_PER_SUBSTATION) + "-" + nodeIndex(node);
    }

    private static String buildingId(int triple) {
        int node = triple / BUILDINGS_PER_NODE;
        return "bld-" + district(node / NODES_PER_SUBSTATION) + "-" + nodeIndex(node) + "-" + buildingIndex(triple);
    }

    private static Map<String, Object> row(Object... keysAndValues) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            row.put((String) keysAndValues[i], keysAndValues[i + 1]);
        }
        return row;
    }
}
