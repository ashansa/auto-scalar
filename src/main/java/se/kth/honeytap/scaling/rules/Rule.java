package se.kth.honeytap.scaling.rules;

import se.kth.honeytap.scaling.monitoring.RuleSupport;


/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class Rule {

    private String ruleName;
    private RuleSupport.ResourceType resourceType;
    private RuleSupport.Comparator comparator;
    private float threshold;
    private int operationAction;  // use x for adding x instances, -x for removing x instances

    public Rule(String ruleName, RuleSupport.ResourceType resourceType, RuleSupport.Comparator comparator, float threshold,
                int operationAction) {
        this.ruleName = ruleName;
        this.resourceType = resourceType;
        this.comparator = comparator;
        this.threshold = threshold;
        this.operationAction = operationAction;
    }

    public RuleSupport.ResourceType getResourceType() {
        return resourceType;
    }

    public void setResourceType(RuleSupport.ResourceType resourceType) {
        this.resourceType = resourceType;
    }

    public RuleSupport.Comparator getComparator() {
        return comparator;
    }

    public void setComparator(RuleSupport.Comparator comparator) {
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

    @Override
    public int hashCode() {
        return ruleName.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Rule) {
            Rule otherRule = (Rule) other;
            return this.ruleName.equals(otherRule.getRuleName());
        } else {
            return false;
        }
    }
}
