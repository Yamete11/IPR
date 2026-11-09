package inno.ipr.ai_incident_responder_service.domain;

import java.util.List;

/**
 * Everything the AGE graph says about an alert's sensor: the asset it monitors, what fails with that
 * asset, who repairs it, and what it depends on.
 *
 * <p>{@link #none()} is the context of a sensor that is not linked in the graph: no asset, nothing
 * impacted, no crews, nothing to correlate with.
 *
 * <p>{@link #linked()} tells whether the sensor resolved to an asset; correlation needs one.
 *
 * <p>{@link #impactedIds()} lists the ids of {@link #impacted()}, the asset first; a new root incident
 * adopts the open incidents on these assets.
 *
 * @param asset          the asset the sensor monitors, or null when the sensor is not in the graph
 * @param impacted       the asset followed by everything downstream of it, deterministically ordered
 * @param crews          crews assigned to any impacted asset
 * @param upstreamRanked assets the asset depends on, most upstream first, ending with the asset itself;
 *                       empty when correlation is off or the sensor is not linked
 */
public record GraphContext(
        ImpactedObject asset,
        List<ImpactedObject> impacted,
        List<Crew> crews,
        List<String> upstreamRanked) {

    public static GraphContext none() {
        return new GraphContext(null, List.of(), List.of(), List.of());
    }

    public boolean linked() {
        return asset != null;
    }

    public List<String> impactedIds() {
        return impacted.stream().map(ImpactedObject::id).toList();
    }
}
