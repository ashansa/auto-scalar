package se.kth.autoscalar.scaling.rules;

import se.kth.autoscalar.scaling.monitoring.RuleSupport.Comparator;
import se.kth.autoscalar.scaling.monitoring.RuleSupport.ResourceType;


/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class Rule {

    private String ruleName;
    private ResourceType resourceType;
    private Comparator comparator;
    private float threshold;
    private int operationAction;  // use x for adding x instances, -x for removing x instances

    public Rule(String ruleName, ResourceType resourceType, Comparator comparator, float threshold,
                int operationAction) {
        this.ruleName = ruleName;
        this.resourceType = resourceType;
        this.comparator = comparator;
        this.threshold = threshold;
        this.operationAction = operationAction;
    }

    public ResourceType getResourceType() {
        return resourceType;
    }

    public void setResourceType(ResourceType resourceType) {
        this.resourceType = resourceType;
    }

    public Comparator getComparator() {
        return comparator;
    }

    public void setComparator(Comparator comparator) {
        this.comparator = comparator;
    }

    public float getThreshold() {
        return threshold;
    }

    public void setThreshold(float threshold) {
        this.threshold = threshold;
    }

    public int getOperationAction() {
        return operationAction;
    }

    public void setOperationAction(int operationAction) {
        this.operationAction = operationAction;
    }

    public String getRuleName() {
        return ruleName;
    }
}
