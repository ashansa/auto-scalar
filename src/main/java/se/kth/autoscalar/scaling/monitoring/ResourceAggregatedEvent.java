package se.kth.autoscalar.scaling.monitoring;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class ResourceAggregatedEvent extends ResourceMonitoringEvent {

    public ResourceAggregatedEvent(String groupId, String machineId, RuleSupport.ResourceType resourceType,
                                   RuleSupport.Comparator comparator, float aggregatedValue) {
        super(groupId, machineId, resourceType, comparator, aggregatedValue);
    }
}
