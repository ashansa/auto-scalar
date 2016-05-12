package se.kth.autoscalar.scaling.monitoring;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class RuleSupport {

    public enum ResourceType {
        CPU, RAM
    }

    public enum Comparator {
        GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL;
    }

    public static Comparator getNormalizedComparatorType(Comparator comparator) {
        if (comparator.equals(Comparator.GREATER_THAN) || comparator.equals(Comparator.GREATER_THAN_OR_EQUAL)) {
            return Comparator.GREATER_THAN_OR_EQUAL;
        } else {
            return Comparator.LESS_THAN_OR_EQUAL;
        }
    }
}
