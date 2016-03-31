package se.kth.autoscalar.scaling.profile;

import se.kth.autoscalar.scaling.monitoring.RuleSupport;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class ProfiledResourceEvent implements ProfiledEvent {

    private String groupId;
    private RuleSupport.ResourceType resourceType;
    private RuleSupport.Comparator comparator;
    private float value;

    public ProfiledResourceEvent(String groupId, RuleSupport.ResourceType resourceType, RuleSupport.Comparator comparator, float value) {
        this.groupId = groupId;
        this.resourceType = resourceType;
        this.comparator = comparator;
        this.value = value;
    }

    public String getGroupId() {
        return groupId;
    }

    public RuleSupport.ResourceType getResourceType() {
        return resourceType;
    }

    public RuleSupport.Comparator getComparator() {
        return comparator;
    }

    public float getValue() {
        return value;
    }
}

