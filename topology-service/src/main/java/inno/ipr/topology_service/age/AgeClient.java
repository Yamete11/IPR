package inno.ipr.topology_service.age;

import inno.ipr.topology_service.exception.AgeQueryException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.regex.Pattern;

/**
 * Thin JDBC wrapper around Apache AGE. Every Cypher statement is run as an SQL
 * {@code SELECT * FROM cypher('<graph>', $$ ... $$) AS (...)} call.
 *
 * <p>The per-connection {@code search_path} (which puts {@code ag_catalog} on the path so
 * {@code cypher()}/{@code create_graph()} resolve) is configured once via Hikari's
 * {@code connection-init-sql} in {@code application.yaml}; this client assumes it is already set.
 *
 * <p>Parameters are passed as a single agtype map (the {@code $key} references inside the
 * statement). The JSON string is bound as {@link Types#OTHER} so PostgreSQL applies the agtype
 * input function via the {@code ?::agtype} cast instead of rejecting a {@code varchar} argument.
 *
 * <p>The graph name is the one value that cannot be bound as a parameter — {@code cypher()} and
 * {@code create_graph()} both take it as a literal — so it is validated against
 * {@link #GRAPH_NAME} before being interpolated. It reaches this class from configuration
 * ({@code topology.graph}), which is not as tightly controlled as the in-repo seed file.
 */
@Component
public class AgeClient {

    /** Graph names are interpolated into SQL/Cypher text, so they must be plain identifiers. */
    private static final Pattern GRAPH_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private final JdbcTemplate jdbc;

    public AgeClient(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Rejects any graph name that is not a bare identifier, so it can be safely interpolated.
     * Called before the surrounding try/catch so the failure is not re-wrapped as a query error.
     */
    private static String checkGraphName(String graph) {
        if (graph == null || !GRAPH_NAME.matcher(graph).matches()) {
            throw new AgeQueryException("Illegal AGE graph name: " + graph);
        }
        return graph;
    }

    /**
     * Creates the AGE graph if it does not exist yet. {@code create_graph} is not idempotent
     * (it errors when the graph is already present), so we probe {@code ag_graph} first.
     */
    public void ensureGraph(String graph) {
        checkGraphName(graph);
        try {
            Integer existing = jdbc.queryForObject(
                    "SELECT count(*) FROM ag_catalog.ag_graph WHERE name = ?",
                    Integer.class, graph);
            if (existing == null || existing == 0) {
                jdbc.execute("SELECT ag_catalog.create_graph('" + graph + "')");
            }
        } catch (RuntimeException e) {
            throw new AgeQueryException("Failed to ensure AGE graph '" + graph + "'", e);
        }
    }

    /**
     * Creates a GIN index over the {@code properties} column of one label's vertex table, so a
     * {@code MATCH (n:Label {id: $id})} lookup — every dependency traversal starts with one — is an
     * index probe rather than a scan of the label table. AGE stores each label as a plain table
     * {@code "<graph>"."<Label>"(id graphid, properties agtype)} that only exists once the first
     * vertex of that label has been written, hence the existence check; on a label the seed never
     * uses this is a no-op. {@code IF NOT EXISTS} makes it idempotent across restarts.
     *
     * <p>{@code label} must already be validated (it comes from the {@code VertexLabel} allow-list),
     * because like the graph name it is interpolated, not bound.
     */
    public void ensurePropertyIndex(String graph, String label) {
        checkGraphName(graph);
        if (label == null || !GRAPH_NAME.matcher(label).matches()) {
            throw new AgeQueryException("Illegal AGE label name: " + label);
        }
        try {
            Integer present = jdbc.queryForObject("""
                    SELECT count(*) FROM ag_catalog.ag_label l
                    JOIN ag_catalog.ag_graph g ON g.graphid = l.graph
                    WHERE g.name = ? AND l.name = ?
                    """, Integer.class, graph, label);
            if (present == null || present == 0) {
                return;
            }
            String index = graph + "_" + label + "_props_gin";
            jdbc.execute("CREATE INDEX IF NOT EXISTS \"" + index + "\" ON \"" + graph + "\".\"" + label
                    + "\" USING gin (properties)");
        } catch (RuntimeException e) {
            throw new AgeQueryException("Failed to ensure property index on '" + graph + "'.'" + label + "'", e);
        }
    }

    /**
     * Runs a write Cypher statement. When {@code paramsJson} is non-null it is passed as the
     * agtype parameter map, referenced as {@code $key} inside {@code cypher}.
     *
     * @return the number of rows the statement produced. A bare write ({@code MERGE} with no
     *         {@code RETURN}) always yields 0; add a {@code RETURN} to the statement when the
     *         caller needs to distinguish "written" from "matched nothing" — a {@code MATCH ...
     *         MERGE} whose {@code MATCH} finds no rows performs no write and reports no error.
     */
    public int execute(String graph, String cypher, String paramsJson) {
        checkGraphName(graph);
        try {
            if (paramsJson == null) {
                String sql = "SELECT * FROM cypher('" + graph + "', $$ " + cypher + " $$) AS (v agtype)";
                return jdbc.query(sql, (ResultSetExtractor<Integer>) AgeClient::countRows);
            }
            String sql = "SELECT * FROM cypher('" + graph + "', $$ " + cypher + " $$, ?::agtype) AS (v agtype)";
            return jdbc.query(sql,
                    ps -> ps.setObject(1, paramsJson, Types.OTHER),
                    (ResultSetExtractor<Integer>) AgeClient::countRows);
        } catch (RuntimeException e) {
            throw new AgeQueryException("Cypher execution failed on graph '" + graph + "': " + cypher, e);
        }
    }

    private static int countRows(ResultSet rs) throws SQLException {
        int rows = 0;
        while (rs.next()) {
            rows++;
        }
        return rows;
    }

    /**
     * Runs a Cypher statement that returns a single scalar count (e.g.
     * {@code MATCH (n) RETURN count(n)}). Used to verify/log graph state after seeding.
     */
    public long count(String graph, String matchReturnCount) {
        checkGraphName(graph);
        try {
            String sql = "SELECT * FROM cypher('" + graph + "', $$ " + matchReturnCount + " $$) AS (c agtype)";
            String result = jdbc.queryForObject(sql, String.class);
            return result == null ? 0L : Long.parseLong(result.trim());
        } catch (RuntimeException e) {
            throw new AgeQueryException("Cypher count failed on graph '" + graph + "': " + matchReturnCount, e);
        }
    }
}
