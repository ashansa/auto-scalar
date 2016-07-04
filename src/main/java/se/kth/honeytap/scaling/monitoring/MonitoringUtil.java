package se.kth.honeytap.scaling.monitoring;

import se.kth.tablespoon.client.events.Comparator;
import se.kth.tablespoon.client.events.Resource;
import se.kth.tablespoon.client.events.ResourceType;
import se.kth.tablespoon.client.events.Threshold;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class MonitoringUtil {

  public static ResourceType getMonitoringResourceType(String type) {
    if (RuleSupport.ResourceType.CPU.name().equals(type)) {
      return ResourceType.CPU;
    }else if (RuleSupport.ResourceType.RAM.name().equals(type)) {
      return ResourceType.CPU;
    } else {
      throw new IllegalArgumentException("No ResourceType found for: " + type);
    }
  }

  public static Threshold getMonitoringThreshold(String comparator, String thresholdString) {
    double threshold = Double.valueOf(thresholdString);
    if (RuleSupport.Comparator.GREATER_THAN.name().equals(comparator)) {
      return new Threshold(threshold, Comparator.GREATER_THAN);
    } else if (RuleSupport.Comparator.GREATER_THAN_OR_EQUAL.name().equals(comparator)) {
      return new Threshold(threshold, Comparator.GREATER_THAN_OR_EQUAL);
    }if (RuleSupport.Comparator.LESS_THAN.name().equals(comparator)) {
      return new Threshold(threshold, Comparator.LESS_THAN);
    }if (RuleSupport.Comparator.LESS_THAN_OR_EQUAL.name().equals(comparator)) {
      return new Threshold(threshold, Comparator.LESS_THAN_OR_EQUAL);
    } else {
      throw new IllegalArgumentException("No Comparator found for: " + comparator);
    }
  }

  public static RuleSupport.ResourceType getASResourceType(String type) {
    if (RuleSupport.ResourceType.CPU.name().equals(type)) {
      return RuleSupport.ResourceType.CPU;
    }else if (RuleSupport.ResourceType.RAM.name().equals(type)) {
      return RuleSupport.ResourceType.CPU;
    } else {
      throw new IllegalArgumentException("No ResourceType found for: " + type);
    }
  }

  public static RuleSupport.Comparator getASComparator(Comparator monitoringComparator) {
    if (Comparator.GREATER_THAN.equals(monitoringComparator)) {
      return RuleSupport.Comparator.GREATER_THAN;
    } else if (Comparator.GREATER_THAN_OR_EQUAL.equals(monitoringComparator)) {
      return RuleSupport.Comparator.GREATER_THAN_OR_EQUAL;
    } if (Comparator.LESS_THAN.equals(monitoringComparator)) {
      return RuleSupport.Comparator.LESS_THAN;
    } else if (Comparator.LESS_THAN_OR_EQUAL.equals(monitoringComparator)) {
      return RuleSupport.Comparator.LESS_THAN_OR_EQUAL;
    } else {
      throw new IllegalArgumentException("No AS Comparator found for: " + monitoringComparator.name());
    }
  }

  public static Comparator getFilteredComparator(Threshold highThreshold, Threshold lowThreshold) {
    if (highThreshold != null && lowThreshold != null) {
      if (Comparator.GREATER_THAN_OR_EQUAL.equals(getNormalizedComparatorType(lowThreshold.comparator)) &&
              Comparator.LESS_THAN_OR_EQUAL.equals(getNormalizedComparatorType(highThreshold.comparator))) {
         //ie:   cpu > 70 and < 80
        return lowThreshold.comparator;
      }
    } else if (lowThreshold == null) {
      return highThreshold.comparator;
    } else if (highThreshold == null) {
      return lowThreshold.comparator;
    }
    throw new IllegalArgumentException("Could not finalize the comparator. Both thresholds are null");
  }

  private static Comparator getNormalizedComparatorType(Comparator comparator) {
    if (comparator.equals(Comparator.GREATER_THAN) || comparator.equals(Comparator.GREATER_THAN_OR_EQUAL)) {
      return Comparator.GREATER_THAN_OR_EQUAL;
    } else {
      return Comparator.LESS_THAN_OR_EQUAL;
    }
  }
}
