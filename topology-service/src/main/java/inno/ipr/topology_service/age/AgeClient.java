package inno.ipr.topology_service.age;

import inno.ipr.topology_service.exception.AgeQueryException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;

import java.sql.Types;

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
 */
@Component
public class AgeClient {

    private final JdbcTemplate jdbc;

    public AgeClient(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Creates the AGE graph if it does not exist yet. {@code create_graph} is not idempotent
     * (it errors when the graph is already present), so we probe {@code ag_graph} first.
     */
    public void ensureGraph(String graph) {
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
     * Runs a write Cypher statement (no {@code RETURN}). When {@code paramsJson} is non-null it is
     * passed as the agtype parameter map, referenced as {@code $key} inside {@code cypher}.
     */
    public void execute(String graph, String cypher, String paramsJson) {
        try {
            if (paramsJson == null) {
                String sql = "SELECT * FROM cypher('" + graph + "', $$ " + cypher + " $$) AS (v agtype)";
                jdbc.query(sql, (ResultSetExtractor<Void>) rs -> null);
            } else {
                String sql = "SELECT * FROM cypher('" + graph + "', $$ " + cypher + " $$, ?::agtype) AS (v agtype)";
                jdbc.query(sql,
                        ps -> ps.setObject(1, paramsJson, Types.OTHER),
                        (ResultSetExtractor<Void>) rs -> null);
            }
        } catch (RuntimeException e) {
            throw new AgeQueryException("Cypher execution failed on graph '" + graph + "': " + cypher, e);
        }
    }

    /**
     * Runs a Cypher statement that returns a single scalar count (e.g.
     * {@code MATCH (n) RETURN count(n)}). Used to verify/log graph state after seeding.
     */
    public long count(String graph, String matchReturnCount) {
        try {
            String sql = "SELECT * FROM cypher('" + graph + "', $$ " + matchReturnCount + " $$) AS (c agtype)";
            String result = jdbc.queryForObject(sql, String.class);
            return result == null ? 0L : Long.parseLong(result.trim());
        } catch (RuntimeException e) {
            throw new AgeQueryException("Cypher count failed on graph '" + graph + "': " + matchReturnCount, e);
        }
    }
}
