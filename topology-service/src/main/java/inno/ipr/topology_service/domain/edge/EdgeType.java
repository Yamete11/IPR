package inno.ipr.topology_service.domain.edge;

/**
 * The closed set of edge (relationship) types in the {@code city_topology} AGE graph.
 *
 * <p>Like {@link inno.ipr.topology_service.domain.vertex.VertexLabel}, a relationship type
 * cannot be parameterised in AGE Cypher, so this enum is the allow-list the seed loader
 * checks before interpolating a type into a statement.
 */
public enum EdgeType {

    POWER_LINE("PowerLine"),
    PIPELINE("Pipeline"),
    SENSOR_LINK("SensorLink");

    private final String type;

    EdgeType(String type) {
        this.type = type;
    }

    /** The relationship type exactly as it appears in Cypher / the seed file. */
    public String type() {
        return type;
    }

    public static EdgeType fromType(String type) {
        for (EdgeType e : values()) {
            if (e.type.equals(type)) {
                return e;
            }
        }
        throw new IllegalArgumentException("Unknown edge type: " + type);
    }
}
