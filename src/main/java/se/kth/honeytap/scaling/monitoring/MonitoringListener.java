package se.kth.honeytap.scaling.monitoring;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.honeytap.scaling.core.HoneyTapAPI;
import se.kth.honeytap.scaling.exceptions.HoneyTapException;
import se.kth.tablespoon.client.api.Subscriber;
import se.kth.tablespoon.client.api.TablespoonEvent;
import se.kth.tablespoon.client.events.EventType;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class MonitoringListener implements Subscriber {

    Log log = LogFactory.getLog(MonitoringListener.class);
    private HoneyTapAPI honeyTapAPI;

    public MonitoringListener(HoneyTapAPI honeyTapAPI) {
        this.honeyTapAPI = honeyTapAPI;
    }

    /*public MonitoringListener(HoneyTapAPI honeyTapAPI, InterestedEvent[] interestedEvents) throws HoneyTapException {
        this.honeyTapAPI = honeyTapAPI;
        this.interestedEvents = new ArrayList<InterestedEvent>(Arrays.asList(interestedEvents));
    }*/

    /*start of resource monitoring related methods*/
    public void onHighCPU(String groupId, ResourceMonitoringEvent event) throws HoneyTapException {
        try {
            honeyTapAPI.handleEvent(groupId, event);
            log.info("@@@@@@@@@@@@@@@ High event received @@@@@@@@@@@@@@@ " + System.currentTimeMillis());
        } catch (HoneyTapException e) {
            log.error("Error while handling onHighCPU event for group: " + groupId + " . " + e.getMessage());
            throw e;
        }
    }

    public void onLowCPU(String groupId, ResourceMonitoringEvent event) throws HoneyTapException {
        try {
            honeyTapAPI.handleEvent(groupId, event);
            log.info("@@@@@@@@@@@@@@@ Low event received @@@@@@@@@@@@@@@ " + System.currentTimeMillis());
        } catch (HoneyTapException e) {
            log.error("Error while handling onHighCPU event for group: " + groupId + " . " + e.getMessage());
            throw e;
        }
    }

    public void onHighRam(String groupId, ResourceMonitoringEvent event) throws HoneyTapException {
        try {
            honeyTapAPI.handleEvent(groupId, event);
            log.info("@@@@@@@@@@@@@@@ High event received @@@@@@@@@@@@@@@ " + System.currentTimeMillis());
        } catch (HoneyTapException e) {
            log.error("Error while handling onHighCPU event for group: " + groupId + " . " + e.getMessage());
            throw e;
        }
    }

    public void onLowRam(String groupId, ResourceMonitoringEvent event) throws HoneyTapException {
        try {
            honeyTapAPI.handleEvent(groupId, event);
            log.info("@@@@@@@@@@@@@@@ Low event received @@@@@@@@@@@@@@@ " + System.currentTimeMillis());
        } catch (HoneyTapException e) {
            log.error("Error while handling onHighCPU event for group: " + groupId + " . " + e.getMessage());
            throw e;
        }
    }

  /*start of machine monitoring related methods*/
    public void onStateChange(String groupId, MachineMonitoringEvent event) throws HoneyTapException {
      try {
          honeyTapAPI.handleEvent(groupId, event);
      } catch (HoneyTapException e) {
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
              } catch (HoneyTapException e) {
                log.error("Error while invoking onHighCPU method for group: " + event.getGroupId());
              }
            } else if (RuleSupport.ResourceType.RAM.equals(resourceType)) {
              try {
                onHighRam(event.getGroupId(), resourceEvent);
              } catch (HoneyTapException e) {
                log.error("Error while invoking onHighRAM method for group: " + event.getGroupId());
              }
            }
          } else if (RuleSupport.Comparator.LESS_THAN_OR_EQUAL.equals(RuleSupport.getNormalizedComparatorType(
                  filteredComparator))) {
            if (RuleSupport.ResourceType.CPU.equals(resourceType)) {
              try {
                onLowCPU(event.getGroupId(), resourceEvent);
              } catch (HoneyTapException e) {
                log.error("Error while invoking onLowCPU method for group: " + event.getGroupId());
              }
            } else if (RuleSupport.ResourceType.RAM.equals(resourceType)) {
              try {
                onLowRam(event.getGroupId(), resourceEvent);
              } catch (HoneyTapException e) {
                log.error("Error while invoking onLowRAM method for group: " + event.getGroupId());
              }
            }
          }
        }

      }

}
