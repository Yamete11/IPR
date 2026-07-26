package inno.ipr.ai_incident_responder_service.ai;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

import java.util.List;

/**
 * The structured remediation plan Claude returns for an alert — the shape from
 * {@code docs/microservices.md}. Passed to the Anthropic SDK as the structured-output type
 * ({@code .outputConfig(IncidentPlan.class)}), so the model's response is constrained to this schema
 * and deserialized into the record directly (no hand-parsing, no risk of malformed JSON). The
 * {@code @Json*Description} annotations become the schema descriptions the model sees.
 *
 * @param criticality      severity level: one of {@code HIGH}, {@code MEDIUM}, {@code LOW}
 * @param summary          one-sentence description of the incident for the dashboard
 * @param impactedObjects  infrastructure objects affected, e.g. {@code ["DistributionNode#5", "Building#12"]}
 * @param remediationSteps ordered, concrete actions for the repair crew
 */
@JsonClassDescription("A structured incident remediation plan for a city-infrastructure failure.")
public record IncidentPlan(
        @JsonPropertyDescription("Severity of the incident: exactly one of HIGH, MEDIUM, or LOW.")
        String criticality,

        @JsonPropertyDescription("A single concise sentence summarising what has failed and its impact.")
        String summary,

        @JsonPropertyDescription("The infrastructure objects affected by this failure, by name/id.")
        List<String> impactedObjects,

        @JsonPropertyDescription("Ordered, concrete remediation steps for the repair crew to follow.")
        List<String> remediationSteps) {
}
