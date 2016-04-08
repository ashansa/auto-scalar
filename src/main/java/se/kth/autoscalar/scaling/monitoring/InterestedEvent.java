package se.kth.autoscalar.scaling.monitoring;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class InterestedEvent {

    public enum X {
        GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL, AVG, TO
    }

    private RuleSupport.ResourceType resourceType;
    //TODO change this as required
    private String interest;   //ie: >= 80 OR <= 30;   avg (10% TO 90%)

    public InterestedEvent(RuleSupport.ResourceType resourceType, String interest) {
        //TODO validate the interest is in correct format
        this.resourceType = resourceType;
        this.interest = interest;
    }

    public RuleSupport.ResourceType getResourceType() {
        return resourceType;
    }

    public String getInterest() {
        return interest;
    }
}
