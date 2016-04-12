package se.kth.autoscalar.scaling.profile;

import se.kth.autoscalar.scaling.monitoring.RuleSupport;

import java.util.HashMap;

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

    private HashMap<String, Float> resourceThresholds;

   /* public ProfiledResourceEvent(String groupId, RuleSupport.ResourceType resourceType, RuleSupport.Comparator comparator, float value) {
        this.groupId = groupId;
        this.resourceType = resourceType;
        this.comparator = comparator;
        this.value = value;
    }*/

    public ProfiledResourceEvent(String groupId) {
        this.groupId = groupId;
        this.resourceThresholds = new HashMap<String, Float>();
    }

    /**
     * Should not add both >= and > for same resource type. Go for highest prioriy
     * @param key combination of resource type and the comparator/AVG separated with a space. ie: CPU >=  OR CPU AVG
     * @param value
     */
    public void addResourceThresholds(String key, Float value) {
        this.resourceThresholds.put(key, value);
    }

    public String getGroupId() {
        return groupId;
    }

    public HashMap<String, Float> getResourceThresholds() {
        return resourceThresholds;
    }

    /*public RuleSupport.ResourceType getResourceType() {
        return resourceType;
    }

    public RuleSupport.Comparator getComparator() {
        return comparator;
    }

    public float getValue() {
        return value;
    }*/
}

