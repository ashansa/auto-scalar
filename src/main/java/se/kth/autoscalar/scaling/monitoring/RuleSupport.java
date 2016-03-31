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
    CPU_PERCENTAGE, RAM_PERCENTAGE
  }

  public enum Comparator {
    GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL;
  }
}
