package inno.ipr.topology_service.age;

import inno.ipr.topology_service.exception.AgeQueryException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.regex.Pattern;

/**
 * Thin JDBC wrapper around Apache AGE. Every Cypher statement is run as an SQL
 * {@code SELECT * FROM cypher('<graph>', $$ ... $$) AS (...)} call. The {@code search_path} that
 * makes {@code cypher()}/{@code create_graph()} resolve is set once per connection by Hikari's
 * {@code connection-init-sql} (see {@code application.yaml}).
 *
 * <p>The graph name, vertex labels, edge types and property names cannot be bound as parameters
 * in AGE, so they are interpolated — after {@link #checkIdentifier(String, String)} has matched
 * them against {@code IDENTIFIER}. The check runs before each method's try/catch, so its failure is
 * not re-wrapped as a query error.
 *
 * <p>{@link #ensureGraph(String)} creates the graph if missing; {@code create_graph} errors when the
 * graph already exists, so {@code ag_graph} is probed first.
 *
 * <p>{@link #ensurePropertyIndex(String, String)} creates a GIN index over one vertex label's
 * {@code properties}, so the {@code MATCH (n:Label {id: $id})} that starts every traversal is an
 * index probe (AGE compiles it to agtype containment {@code @>}, which GIN supports).
 *
 * <p>{@link #ensureEdgeIndexes(String, String)} creates B-tree indexes on {@code start_id} and
 * {@code end_id} of one edge type's table. AGE creates none, so without them every hop of a pattern
 * scans the edge table, in both directions ({@code dependents} walks start → end,
 * {@code dependencies} end → start).
 *
 * <p>Both index methods are idempotent ({@code IF NOT EXISTS}) and no-ops for a label whose table
 * does not exist yet: AGE only materialises {@code "<graph>"."<Label>"} with its first vertex/edge,
 * which {@code labelExists(String, String)} checks.
 *
 * <p>{@link #execute(String, String, String)} runs a write statement. A non-null {@code paramsJson}
 * is the agtype map behind the statement's {@code $key} references, bound as {@link Types#OTHER} so
 * the {@code ?::agtype} cast applies AGE's input function. It returns the number of rows produced:
 * a bare write ({@code MERGE} without {@code RETURN}) always yields 0, so add a {@code RETURN} when
 * the caller must tell "written" from "{@code MATCH} found nothing" — the latter reports no error.
 *
 * <p>{@link #count(String, String)} runs a statement returning one scalar count, used to log graph
 * state after seeding.
 *
 * <p>{@link #checkIdentifier(String, String)} rejects anything that is not a plain identifier;
 * {@code kind} ("graph", "label", "property") only shapes the error message.
 */
@Component
public class AgeClient {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private final JdbcTemplate jdbc;

    public AgeClient(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void ensureGraph(String graph) {
        checkIdentifier("graph", graph);
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

    public void ensurePropertyIndex(String graph, String label) {
        checkIdentifier("graph", graph);
        checkIdentifier("label", label);
        try {
            if (labelExists(graph, label)) {
                createIndex(graph + "_" + label + "_props_gin", graph, label, "USING gin (properties)");
            }
        } catch (RuntimeException e) {
            throw new AgeQueryException("Failed to ensure property index on '" + graph + "'.'" + label + "'", e);
        }
    }

    public void ensureEdgeIndexes(String graph, String type) {
        checkIdentifier("graph", graph);
        checkIdentifier("label", type);
        try {
            if (labelExists(graph, type)) {
                createIndex(graph + "_" + type + "_start_id", graph, type, "(start_id)");
                createIndex(graph + "_" + type + "_end_id", graph, type, "(end_id)");
            }
        } catch (RuntimeException e) {
            throw new AgeQueryException("Failed to ensure edge indexes on '" + graph + "'.'" + type + "'", e);
        }
    }

    public int execute(String graph, String cypher, String paramsJson) {
        checkIdentifier("graph", graph);
        boolean hasParams = paramsJson != null;
        String sql = "SELECT * FROM cypher('" + graph + "', $$ " + cypher + " $$"
                + (hasParams ? ", ?::agtype" : "") + ") AS (v agtype)";
        PreparedStatementSetter bind = ps -> {
            if (hasParams) {
                ps.setObject(1, paramsJson, Types.OTHER);
            }
        };
        try {
            return jdbc.query(sql, bind, (ResultSetExtractor<Integer>) AgeClient::countRows);
        } catch (RuntimeException e) {
            throw new AgeQueryException("Cypher execution failed on graph '" + graph + "': " + cypher, e);
        }
    }

    public long count(String graph, String matchReturnCount) {
        checkIdentifier("graph", graph);
        try {
            String sql = "SELECT * FROM cypher('" + graph + "', $$ " + matchReturnCount + " $$) AS (c agtype)";
            String result = jdbc.queryForObject(sql, String.class);
            return result == null ? 0L : Long.parseLong(result.trim());
        } catch (RuntimeException e) {
            throw new AgeQueryException("Cypher count failed on graph '" + graph + "': " + matchReturnCount, e);
        }
    }

    public static void checkIdentifier(String kind, String name) {
        if (name == null || !IDENTIFIER.matcher(name).matches()) {
            throw new AgeQueryException("Illegal AGE " + kind + " name: " + name);
        }
    }

    private boolean labelExists(String graph, String label) {
        Integer present = jdbc.queryForObject("""
                SELECT count(*) FROM ag_catalog.ag_label l
                JOIN ag_catalog.ag_graph g ON g.graphid = l.graph
                WHERE g.name = ? AND l.name = ?
                """, Integer.class, graph, label);
        return present != null && present > 0;
    }

    private void createIndex(String index, String graph, String table, String definition) {
        jdbc.execute("CREATE INDEX IF NOT EXISTS \"" + index + "\" ON \"" + graph + "\".\"" + table + "\" "
                + definition);
    }

    private static int countRows(ResultSet rs) throws SQLException {
        int rows = 0;
        while (rs.next()) {
            rows++;
        }
        return rows;
    }
}
