package inno.ipr.ai_incident_responder_service.ai;

import inno.ipr.ai_incident_responder_service.alert.Alert;
import inno.ipr.ai_incident_responder_service.domain.Crew;
import inno.ipr.ai_incident_responder_service.domain.ImpactedObject;
import inno.ipr.ai_incident_responder_service.domain.MetricSummary;

import java.util.List;

/**
 * Assembles the lightweight-RAG prompt for the Claude call. The prompt is split in two:
 * {@link #SYSTEM_PROMPT} — the fixed role and rules, identical on every request — and
 * {@link #build} — the per-alert user message with the raw alert, a one-hour metric summary, the
 * dependency-graph objects the failure impacts and the repair crews assigned to them.
 *
 * <p>The user message is bounded so it stays well within token limits regardless of ingestion
 * volume or graph size: the history is a fixed-size summary, never the raw series, and at most
 * {@link #MAX_IMPACTED_IN_PROMPT} impacted objects are listed (the rest are counted, not dropped
 * silently). Pure and stateless so the exact text can be asserted in tests.
 *
 * <p>{@link #SYSTEM_PROMPT} holds the model's role and output rules. It is sent as the request's
 * {@code system} parameter rather than inside the user message: it is operator instruction, not
 * data, and keeping it byte-identical across requests is what would let it be prompt-cached once it
 * grows past the model's minimum cacheable prefix.
 *
 * <p>{@link #MAX_IMPACTED_IN_PROMPT} is 50 because one line is ~15 tokens, so 50 lines keep the
 * graph section under ~1k tokens even for a substation that feeds the whole city; beyond that the
 * plan does not improve, it only costs more and dilutes the context.
 *
 * <p>{@link #MAX_CREW_ASSETS_IN_PROMPT} caps the assets listed per crew (10, the rest counted): a
 * district crew covers every asset of its district, and the model needs who to call, not the full
 * inventory. Crews come from the graph and the relational assignment tables, so the system prompt
 * tells the model to address steps to them and not to invent others.
 */
public final class IncidentPromptBuilder {

    public static final String SYSTEM_PROMPT = """
            You are the incident-response coordinator for a Smart City infrastructure network \
            (power substations, distribution nodes, residential buildings and their sensors). \
            For each monitoring alert you receive, produce a structured remediation plan for the \
            field repair crew.

            Rules:
            - Base the plan only on the alert, the metric history and the dependency graph given \
            in the message; do not invent objects that are not listed.
            - Criticality is exactly one of HIGH, MEDIUM or LOW. Rate higher when residential \
            buildings lose service or the latest reading is still outside the safe range.
            - The summary is one sentence for the operator dashboard.
            - Remediation steps are ordered, concrete actions a crew can carry out, starting with \
            making the site safe.
            - Address the steps to the responsible crews listed in the message, by name; do not \
            invent crews that are not listed.""";

    static final int MAX_IMPACTED_IN_PROMPT = 50;
    static final int MAX_CREW_ASSETS_IN_PROMPT = 10;

    private IncidentPromptBuilder() {
    }

    public static String build(IncidentContext context) {
        Alert alert = context.alert();
        MetricSummary history = context.history();
        List<ImpactedObject> impacted = context.impacted();
        StringBuilder sb = new StringBuilder();
        sb.append("A monitoring rule has fired. Produce the remediation plan from the context below.\n\n");

        sb.append("## Alert\n")
                .append("- sensor_id: ").append(alert.sensorId()).append('\n')
                .append("- metric_type: ").append(alert.metricType()).append('\n')
                .append("- value: ").append(alert.value()).append('\n')
                .append("- rule: ").append(alert.rule()).append('\n')
                .append("- detected_at: ").append(alert.detectedAt()).append("\n\n");

        sb.append("## Last hour of readings for this sensor\n");
        if (history.isEmpty()) {
            sb.append("- no readings recorded in the last hour\n");
        } else {
            sb.append("- count: ").append(history.count()).append('\n')
                    .append("- min: ").append(history.min()).append('\n')
                    .append("- max: ").append(history.max()).append('\n')
                    .append("- avg: ").append(history.avg()).append('\n')
                    .append("- latest: ").append(history.latest())
                    .append(" (at ").append(history.latestAt()).append(")\n");
        }
        sb.append('\n');

        sb.append("## Impacted objects (from the network dependency graph)\n");
        if (impacted.isEmpty()) {
            sb.append("- none resolved (sensor not linked in the topology graph)\n");
        } else {
            for (ImpactedObject o : impacted.subList(0, Math.min(impacted.size(), MAX_IMPACTED_IN_PROMPT))) {
                Object name = o.properties() == null ? null : o.properties().get("name");
                sb.append("- ").append(o.label()).append(' ').append(o.id());
                if (name != null) {
                    sb.append(" (").append(name).append(')');
                }
                sb.append('\n');
            }
            if (impacted.size() > MAX_IMPACTED_IN_PROMPT) {
                sb.append("- ... and ").append(impacted.size() - MAX_IMPACTED_IN_PROMPT)
                        .append(" more downstream objects (").append(impacted.size())
                        .append(" in total; list truncated to fit the prompt)\n");
            }
        }
        sb.append('\n');

        sb.append("## Responsible crews\n");
        if (context.crews().isEmpty()) {
            sb.append("- none assigned\n");
        } else {
            for (Crew crew : context.crews()) {
                List<String> assets = crew.assets();
                sb.append("- ").append(crew.name()).append(" (").append(crew.crewId());
                if (crew.phone() != null) {
                    sb.append(", ").append(crew.phone());
                }
                sb.append("): ").append(String.join(", ",
                        assets.subList(0, Math.min(assets.size(), MAX_CREW_ASSETS_IN_PROMPT))));
                if (assets.size() > MAX_CREW_ASSETS_IN_PROMPT) {
                    sb.append(" and ").append(assets.size() - MAX_CREW_ASSETS_IN_PROMPT).append(" more");
                }
                sb.append('\n');
            }
        }
        sb.append('\n');

        sb.append("Assess criticality (HIGH/MEDIUM/LOW), summarise the incident in one sentence, ")
                .append("list the impacted objects, and give ordered, concrete remediation steps ")
                .append("addressed to the responsible crews.");
        return sb.toString();
    }
}
