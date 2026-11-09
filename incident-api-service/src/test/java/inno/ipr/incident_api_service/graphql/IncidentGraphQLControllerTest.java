package inno.ipr.incident_api_service.graphql;

import inno.ipr.incident_api_service.graphql.dto.CrewDto;
import inno.ipr.incident_api_service.graphql.dto.IncidentDto;
import inno.ipr.incident_api_service.graphql.dto.MetricDto;
import inno.ipr.incident_api_service.repository.IncidentReadRepository;
import inno.ipr.incident_api_service.repository.MetricReadRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.graphql.test.autoconfigure.GraphQlTest;
import org.springframework.graphql.test.tester.GraphQlTester;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

/**
 * The GraphQL contract over mocked repositories: schema defaults reach the repositories, client
 * limits are clamped before they become a {@code LIMIT ?}, and the JSONB-backed lists surface as
 * {@code [String!]!}.
 */
@GraphQlTest(IncidentGraphQLController.class)
class IncidentGraphQLControllerTest {

    private static final IncidentDto INCIDENT = new IncidentDto(
            "i-17155ca7", "a-1001", "s-00042", "pipe_pressure", "node-5", null, "2026-09-06T09:59:50Z", "HIGH",
            List.of("DistributionNode node-5", "ResidentialBuilding bld-12"),
            List.of("Dispatch a repair crew to node-5.", "Confirm service is restored."),
            List.of(new CrewDto("crew-grid", "Distribution grid crew", "+7-900-000-0102", List.of("node-5"))),
            "Zero pressure at node-5", "mock", "2026-09-06T10:00:00Z");
    private static final IncidentDto CHILD = new IncidentDto(
            "i-child", "a-1002", "s-01000", "pipe_pressure", "bld-12", "i-17155ca7", "2026-09-06T10:00:05Z", "HIGH",
            List.of(), List.of(), List.of(), "Secondary effect", "correlated", "2026-09-06T10:00:06Z");

    @Autowired
    private GraphQlTester graphQl;

    @MockitoBean
    private IncidentReadRepository incidents;

    @MockitoBean
    private MetricReadRepository metrics;

    @Test
    @DisplayName("incidents uses the schema default limit and maps every field, lists included")
    void incidentsDefaultLimit() {
        given(incidents.findRecent(anyInt())).willReturn(List.of(INCIDENT));

        graphQl.document("""
                { incidents { incidentId alertId sensorId metricType criticality
                              impactedObjects remediationSteps summary model createdAt } }
                """)
                .execute()
                .path("incidents").entityList(IncidentDto.class).hasSize(1)
                .path("incidents[0].impactedObjects").entityList(String.class)
                .containsExactly("DistributionNode node-5", "ResidentialBuilding bld-12")
                .path("incidents[0].remediationSteps").entityList(String.class).hasSize(2)
                .path("incidents[0].criticality").entity(String.class).isEqualTo("HIGH");

        verify(incidents).findRecent(50);
    }

    @Test
    @DisplayName("an oversized limit is clamped to 1000 and a negative one to 0")
    void limitsAreClamped() {
        given(incidents.findRecent(anyInt())).willReturn(List.of());

        graphQl.document("{ incidents(limit: 5000) { incidentId } }").execute()
                .path("incidents").entityList(IncidentDto.class).hasSize(0);
        verify(incidents).findRecent(1000);

        graphQl.document("{ incidents(limit: -7) { incidentId } }").execute()
                .path("incidents").entityList(IncidentDto.class).hasSize(0);
        verify(incidents).findRecent(0);
    }

    @Test
    @DisplayName("incidentsBySensor is bounded too: default 50, clamped like the rest")
    void incidentsBySensorIsBounded() {
        given(incidents.findBySensor(eq("s-00042"), anyInt())).willReturn(List.of(INCIDENT));

        graphQl.document("{ incidentsBySensor(sensorId: \"s-00042\") { incidentId } }").execute()
                .path("incidentsBySensor[0].incidentId").entity(String.class).isEqualTo("i-17155ca7");
        verify(incidents).findBySensor("s-00042", 50);

        graphQl.document("{ incidentsBySensor(sensorId: \"s-00042\", limit: 99999) { incidentId } }").execute()
                .path("incidentsBySensor").entityList(IncidentDto.class).hasSize(1);
        verify(incidents).findBySensor("s-00042", 1000);
    }

    @Test
    @DisplayName("an unknown incident id resolves to null, not an error")
    void unknownIncidentIsNull() {
        given(incidents.findById("i-missing")).willReturn(null);

        graphQl.document("{ incident(id: \"i-missing\") { incidentId } }").execute()
                .errors().verify()
                .path("incident").valueIsNull();
    }

    @Test
    @DisplayName("recentMetrics maps readings and clamps its limit")
    void recentMetrics() {
        given(metrics.findRecentBySensor(eq("s-00042"), anyInt())).willReturn(List.of(
                new MetricDto("2026-09-06T10:00:00Z", "s-00042", "pipe_pressure", 0.0, "CRITICAL")));

        graphQl.document("{ recentMetrics(sensorId: \"s-00042\", limit: 3000) { timestamp sensorId metricType value status } }")
                .execute()
                .path("recentMetrics").entityList(MetricDto.class).hasSize(1)
                .path("recentMetrics[0].value").entity(Double.class).satisfies(v -> assertThat(v).isZero())
                .path("recentMetrics[0].status").entity(String.class).isEqualTo("CRITICAL");

        verify(metrics).findRecentBySensor("s-00042", 1000);
    }

    @Test
    @DisplayName("an incident exposes its crews, its root and the incidents filed under it")
    void rootCauseLinks() {
        given(incidents.findById("i-child")).willReturn(CHILD);
        given(incidents.findById("i-17155ca7")).willReturn(INCIDENT);
        given(incidents.findByRoot(eq("i-17155ca7"), anyInt())).willReturn(List.of(CHILD));

        graphQl.document("""
                { incident(id: "i-child") { assetId rootIncidentId model
                    rootIncident { incidentId assetId crews { crewId name phone assets }
                                   linkedIncidents { incidentId } } } }
                """)
                .execute()
                .path("incident.assetId").entity(String.class).isEqualTo("bld-12")
                .path("incident.model").entity(String.class).isEqualTo("correlated")
                .path("incident.rootIncident.incidentId").entity(String.class).isEqualTo("i-17155ca7")
                .path("incident.rootIncident.crews[0].name").entity(String.class).isEqualTo("Distribution grid crew")
                .path("incident.rootIncident.crews[0].assets").entityList(String.class).containsExactly("node-5")
                .path("incident.rootIncident.linkedIncidents[0].incidentId").entity(String.class).isEqualTo("i-child");

        verify(incidents).findByRoot("i-17155ca7", 100);
    }

    @Test
    @DisplayName("a root incident has no rootIncident")
    void rootHasNoRoot() {
        given(incidents.findById("i-17155ca7")).willReturn(INCIDENT);

        graphQl.document("{ incident(id: \"i-17155ca7\") { rootIncident { incidentId } } }").execute()
                .path("incident.rootIncident").valueIsNull();
    }
}
