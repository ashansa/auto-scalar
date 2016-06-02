package se.kth.honeytap.scaling.monitoring;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class InterestedEvent {

    /*public enum X {
        GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL, AVG, TO
    }
    */

    //TODO change this as required
    private String interest;   //resource interests: CPU:>=:80; RAM:<=:30;   CPU:AVG:>=:10:<=:90  ; CPU:>=:70:<=:80
                               // machine interests: KILLED; AT_END_OF_BILLING_PERIOD
    private RuleSupport.Comparator comparator;

    public InterestedEvent(String interest) {
        //TODO validate the interest is in correct format
        this.interest = interest;
    }

    public InterestedEvent(String interest, RuleSupport.Comparator comparator) {
        //TODO validate the interest is in correct format
        this.interest = interest;
        this.comparator = comparator;
    }

    public String getInterest() {
        return interest;
    }

    public RuleSupport.Comparator getComparator() {
        return comparator;
    }
}
