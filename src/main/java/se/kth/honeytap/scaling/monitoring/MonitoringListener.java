package se.kth.honeytap.scaling.monitoring;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.honeytap.scaling.core.HoneyTapAPI;
import se.kth.honeytap.scaling.exceptions.HoneyTapException;
import se.kth.honeytap.stat.StatManager;
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
          StatManager.addCuChanges(System.currentTimeMillis(), "high",event.getCurrentValue());
        } catch (HoneyTapException e) {
            log.error("Error while handling onHighCPU event for group: " + groupId + " . " + e.getMessage());
            throw e;
        }
    }

    public void onLowCPU(String groupId, ResourceMonitoringEvent event) throws HoneyTapException {
        try {
            honeyTapAPI.handleEvent(groupId, event);
            log.info("@@@@@@@@@@@@@@@ Low event received @@@@@@@@@@@@@@@ " + System.currentTimeMillis());
          StatManager.addCuChanges(System.currentTimeMillis(), "low",event.getCurrentValue());
        } catch (HoneyTapException e) {
            log.error("Error while handling onHighCPU event for group: " + groupId + " . " + e.getMessage());
            throw e;
        }
    }

    public void onHighRam(String groupId, ResourceMonitoringEvent event) throws HoneyTapException {
        try {
            honeyTapAPI.handleEvent(groupId, event);
            log.info("@@@@@@@@@@@@@@@ High event received @@@@@@@@@@@@@@@ " + System.currentTimeMillis());
          StatManager.addRamChanges(System.currentTimeMillis(), "high",event.getCurrentValue());
        } catch (HoneyTapException e) {
            log.error("Error while handling onHighCPU event for group: " + groupId + " . " + e.getMessage());
            throw e;
        }
    }

    public void onLowRam(String groupId, ResourceMonitoringEvent event) throws HoneyTapException {
        try {
            honeyTapAPI.handleEvent(groupId, event);
            log.info("@@@@@@@@@@@@@@@ Low event received @@@@@@@@@@@@@@@ " + System.currentTimeMillis());
          StatManager.addRamChanges(System.currentTimeMillis(), "low",event.getCurrentValue());
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
        log.info("==================== table spoon event arrived ==================");
        logTSEvent(event);
        log.info("==================== table spoon event logged  ==================");

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

  private void logTSEvent(TablespoonEvent event) {
    log.info("######################################################");
    log.info("################### logging TS event #################");
    log.info("######################################################");
    log.info("group: " + event.getGroupId());
    log.info("machine: " + event.getMachineId());
    log.info("value: " + event.getValue());
    log.info("time: " + event.getTimeStamp());
    log.info("coll index: " + event.getCollectIndex());
    log.info("event type: " + event.getEventType().name());
    log.info("resource type : " + event.getResourceType().name());
    if (event.getHigh() != null) {
      log.info("threshold High: " + event.getHigh().toString());
    } else {
      log.info("threshold High: null");
    }

    if (event.getLow() != null) {
      log.info("threshold High: " + event.getLow().toString());
    } else {
      log.info("threshold Low: null");
    }

    log.info("######################## logged ######################");

  }
}
