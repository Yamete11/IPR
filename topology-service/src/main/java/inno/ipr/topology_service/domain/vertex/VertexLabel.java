package inno.ipr.topology_service.domain.vertex;

/**
 * The closed set of vertex labels in the {@code city_topology} AGE graph.
 *
 * <p>AGE cannot parameterise a node label — it must be a literal inside the Cypher text.
 * This enum is the allow-list the seed loader validates every incoming label against before
 * interpolating it into a statement, so a label coming from the seed file can never inject
 * arbitrary Cypher.
 */
public enum VertexLabel {

    SUBSTATION("Substation"),
    DISTRIBUTION_NODE("DistributionNode"),
    RESIDENTIAL_BUILDING("ResidentialBuilding"),
    SENSOR("Sensor");

    private final String label;

    VertexLabel(String label) {
        this.label = label;
    }

    /** The label exactly as it appears in Cypher / the seed file. */
    public String label() {
        return label;
    }

    public static VertexLabel fromLabel(String label) {
        for (VertexLabel v : values()) {
            if (v.label.equals(label)) {
                return v;
            }
        }
        throw new IllegalArgumentException("Unknown vertex label: " + label);
    }
}
