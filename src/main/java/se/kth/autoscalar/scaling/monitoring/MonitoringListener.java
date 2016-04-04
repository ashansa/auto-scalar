package se.kth.autoscalar.scaling.monitoring;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.autoscalar.scaling.core.AutoScalarAPI;
import se.kth.autoscalar.scaling.exceptions.AutoScalarException;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class MonitoringListener {

    Log log = LogFactory.getLog(MonitoringListener.class);

    private ArrayList<InterestedEvent> interestedEvents;
    private AutoScalarAPI autoScalarAPI;

    public MonitoringListener(AutoScalarAPI autoScalarAPI) throws AutoScalarException {
        this.autoScalarAPI = autoScalarAPI;
    }

    public MonitoringListener(AutoScalarAPI autoScalarAPI, InterestedEvent[] interestedEvents) throws AutoScalarException {
        this.autoScalarAPI = autoScalarAPI;
        this.interestedEvents = new ArrayList<InterestedEvent>(Arrays.asList(interestedEvents));
    }

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

}
