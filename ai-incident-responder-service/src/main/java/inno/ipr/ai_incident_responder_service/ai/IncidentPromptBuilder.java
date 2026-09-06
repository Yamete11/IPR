package inno.ipr.ai_incident_responder_service.ai;

import inno.ipr.ai_incident_responder_service.alert.Alert;
import inno.ipr.ai_incident_responder_service.domain.ImpactedObject;
import inno.ipr.ai_incident_responder_service.domain.MetricSummary;

import java.util.List;

/**
 * Assembles the lightweight-RAG prompt for the Claude call: the crew's role, the raw alert, a
 * one-hour metric summary, and the dependency-graph objects the failure impacts. Kept compact so
 * it stays well within token limits regardless of ingestion volume — the history is a fixed-size
 * summary, never the raw series. Pure and stateless so the exact text can be asserted in tests.
 */
public final class IncidentPromptBuilder {

    private IncidentPromptBuilder() {
    }

    public static String build(Alert alert, MetricSummary history, List<ImpactedObject> impacted) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are the incident-response coordinator for a Smart City infrastructure network. ")
                .append("A monitoring rule has fired. Using the context below, produce a structured ")
                .append("remediation plan for the field repair crew.\n\n");

        sb.append("## Alert\n")
                .append("- sensor_id: ").append(alert.sensorId()).append('\n')
                .append("- metric_type: ").append(alert.metricType()).append('\n')
                .append("- value: ").append(alert.value()).append('\n')
                .append("- rule: ").append(alert.rule()).append('\n')
                .append("- detected_at: ").append(alert.detectedAt()).append("\n\n");

        sb.append("## Last hour of readings for this sensor\n");
        if (history == null || history.isEmpty()) {
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
        if (impacted == null || impacted.isEmpty()) {
            sb.append("- none resolved (sensor not linked in the topology graph)\n");
        } else {
            for (ImpactedObject o : impacted) {
                Object name = o.properties() == null ? null : o.properties().get("name");
                sb.append("- ").append(o.label()).append(' ').append(o.id());
                if (name != null) {
                    sb.append(" (").append(name).append(')');
                }
                sb.append('\n');
            }
        }
        sb.append('\n');

        sb.append("Assess criticality (HIGH/MEDIUM/LOW), summarise the incident in one sentence, ")
                .append("list the impacted objects, and give ordered, concrete remediation steps.");
        return sb.toString();
    }
}
