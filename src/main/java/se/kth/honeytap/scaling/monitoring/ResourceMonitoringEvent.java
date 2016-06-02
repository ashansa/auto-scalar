package se.kth.honeytap.scaling.monitoring;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class ResourceMonitoringEvent extends MonitoringEvent {

    private RuleSupport.ResourceType resourceType;
    private RuleSupport.Comparator comparator;
    private float currentValue;

    public ResourceMonitoringEvent(String groupId, String machineId, RuleSupport.ResourceType resourceType,
                                   RuleSupport.Comparator comparator, float value) {
        super(groupId, machineId);
        this.resourceType = resourceType;
        this.comparator = comparator;
        this.currentValue = value;
    }

    public RuleSupport.ResourceType getResourceType() {
        return resourceType;
    }

    public RuleSupport.Comparator getComparator() {
        return comparator;
    }

    public float getCurrentValue() {
        return currentValue;
    }
}
