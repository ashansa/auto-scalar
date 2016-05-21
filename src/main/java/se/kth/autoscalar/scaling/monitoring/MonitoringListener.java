package se.kth.autoscalar.scaling.monitoring;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.autoscalar.scaling.core.AutoScalarAPI;
import se.kth.autoscalar.scaling.exceptions.AutoScalarException;
import se.kth.tablespoon.client.api.Event;
import se.kth.tablespoon.client.api.Subscriber;
import se.kth.tablespoon.client.api.TablespoonEvent;
import se.kth.tablespoon.client.topics.Comparator;
import se.kth.tablespoon.client.topics.EventType;
import se.kth.tablespoon.client.topics.Threshold;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class MonitoringListener implements Subscriber {

    Log log = LogFactory.getLog(MonitoringListener.class);
    private AutoScalarAPI autoScalarAPI;

    public MonitoringListener(AutoScalarAPI autoScalarAPI) {
        this.autoScalarAPI = autoScalarAPI;
    }

    /*public MonitoringListener(AutoScalarAPI autoScalarAPI, InterestedEvent[] interestedEvents) throws AutoScalarException {
        this.autoScalarAPI = autoScalarAPI;
        this.interestedEvents = new ArrayList<InterestedEvent>(Arrays.asList(interestedEvents));
    }*/

    /*start of resource monitoring related methods*/
    public void onHighCPU(String groupId, ResourceMonitoringEvent event) throws AutoScalarException {
        try {
            autoScalarAPI.handleEvent(groupId, event);
        } catch (AutoScalarException e) {
            log.error("Error while handling onHighCPU event for group: " + groupId + " . " + e.getMessage());
            throw e;
        }
    }

    public void onLowCPU(String groupId, ResourceMonitoringEvent event) throws AutoScalarException {
        try {
            autoScalarAPI.handleEvent(groupId, event);
        } catch (AutoScalarException e) {
            log.error("Error while handling onHighCPU event for group: " + groupId + " . " + e.getMessage());
            throw e;
        }
    }

    public void onHighRam(String groupId, ResourceMonitoringEvent event) throws AutoScalarException {
        try {
            autoScalarAPI.handleEvent(groupId, event);
        } catch (AutoScalarException e) {
            log.error("Error while handling onHighCPU event for group: " + groupId + " . " + e.getMessage());
            throw e;
        }
    }

    public void onLowRam(String groupId, ResourceMonitoringEvent event) throws AutoScalarException {
        try {
            autoScalarAPI.handleEvent(groupId, event);
        } catch (AutoScalarException e) {
            log.error("Error while handling onHighCPU event for group: " + groupId + " . " + e.getMessage());
            throw e;
        }
    }

  /*start of machine monitoring related methods*/
    public void onStateChange(String groupId, MachineMonitoringEvent event) throws AutoScalarException {
      try {
          autoScalarAPI.handleEvent(groupId, event);
      } catch (AutoScalarException e) {
          log.error("Error while handling stateChange event for group: " + groupId + " with state change: " +
                  event.getStatus() + " . " + e.getMessage());
          throw e;
      }
  }

      @Override
      public void onEventArrival(TablespoonEvent event) {
        System.out.println("==================== table spoon event arrived ==================");
        if (EventType.REGULAR.equals(event.getEventType())) {
          RuleSupport.Comparator filteredComparator = MonitoringUtil.getASComparator(MonitoringUtil.
                  getFilteredComparator(event.getHigh(), event.getLow()));
          RuleSupport.ResourceType resourceType = MonitoringUtil.getASResourceType(event.getResourceType().name());
          ResourceMonitoringEvent resourceEvent = new ResourceMonitoringEvent(event.getGroupId(), "machineId",
                  resourceType, filteredComparator, (float) event.getValue());
          if (RuleSupport.Comparator.GREATER_THAN_OR_EQUAL.equals(RuleSupport.getNormalizedComparatorType(
                  filteredComparator))) {
            if (RuleSupport.ResourceType.CPU.equals(resourceType)) {
              try {
                onHighCPU(event.getGroupId(), resourceEvent);
              } catch (AutoScalarException e) {
                log.error("Error while invoking onHighCPU method for group: " + event.getGroupId());
              }
            } else if (RuleSupport.ResourceType.RAM.equals(resourceType)) {
              try {
                onHighRam(event.getGroupId(), resourceEvent);
              } catch (AutoScalarException e) {
                log.error("Error while invoking onHighRAM method for group: " + event.getGroupId());
              }
            }
          } else if (RuleSupport.Comparator.LESS_THAN_OR_EQUAL.equals(RuleSupport.getNormalizedComparatorType(
                  filteredComparator))) {
            if (RuleSupport.ResourceType.CPU.equals(resourceType)) {
              try {
                onLowCPU(event.getGroupId(), resourceEvent);
              } catch (AutoScalarException e) {
                log.error("Error while invoking onLowCPU method for group: " + event.getGroupId());
              }
            } else if (RuleSupport.ResourceType.RAM.equals(resourceType)) {
              try {
                onLowRam(event.getGroupId(), resourceEvent);
              } catch (AutoScalarException e) {
                log.error("Error while invoking onLowRAM method for group: " + event.getGroupId());
              }
            }
          }
        }

      }

}
