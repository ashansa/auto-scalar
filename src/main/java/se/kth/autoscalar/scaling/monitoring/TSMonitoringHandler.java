package se.kth.autoscalar.scaling.monitoring;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.autoscalar.scaling.Constants;
import se.kth.autoscalar.scaling.core.AutoScalarAPI;
import se.kth.autoscalar.scaling.exceptions.AutoScalarException;
import se.kth.tablespoon.client.api.TablespoonAPI;
import se.kth.tablespoon.client.events.EventType;
import se.kth.tablespoon.client.topics.ThresholdException;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class TSMonitoringHandler implements MonitoringHandler{

    Log log = LogFactory.getLog(TSMonitoringHandler.class);
    private AutoScalarAPI autoScalarAPI;
    private TablespoonAPI tablespoonAPI;
    MonitoringListener monitoringListener;

    public TSMonitoringHandler(AutoScalarAPI autoScalarAPI) {
        this.autoScalarAPI = autoScalarAPI;
        this.tablespoonAPI = TablespoonAPI.getInstance();
        this.monitoringListener = new MonitoringListener(this.autoScalarAPI);
    }

    public MonitoringListener addGroupForMonitoring(String groupId, InterestedEvent[] interestedEvents) throws AutoScalarException {
        for (InterestedEvent interestedEvent : interestedEvents) {
            //3 forms of items   RAM:<=:30;   CPU:AVG:>=:10:<=:90;    KILLED;
            //but the initial form will be always the 1st form
            try {
                String[] items = interestedEvent.getInterest().split(Constants.SEPARATOR);

                tablespoonAPI.createTopic(monitoringListener, groupId, EventType.REGULAR, MonitoringUtil.
                        getMonitoringResourceType(items[0]), 0, 1, MonitoringUtil.getMonitoringThreshold(
                        items[1], items[2]));
            } catch (Exception e) {
                log.warn("Could not add the monitoring event: " + interestedEvent.getInterest() + " for group: " +
                        groupId, e);
            }
        }

        return monitoringListener;
    }

    public void removeGroupFromMonitoring(String groupId) {
        // avoid monitoring and sending events of the machines of the group
    }

    public void addInterestedEvent(String groupId, InterestedEvent[] events, int timeDuration) {
        //TODO request events for a limited time
        //should handle all interested event types
        //resource interests: CPU:>=:80; RAM:<=:30;   CPU:AVG:>=:10:<=:90  ; CPU:>=:70:<=:80
        // machine interests: KILLED; AT_END_OF_BILLING_PERIOD
        for (InterestedEvent event : events) {
            try {
                String items[] = event.getInterest().split(Constants.SEPARATOR);
                if (event.getInterest().contains(Constants.AVERAGE) && items.length == 6) {  //ie: CPU:AVG:>=:10:<=:90
                    tablespoonAPI.createTopic(monitoringListener, groupId, EventType.GROUP_AVERAGE, MonitoringUtil.
                            getMonitoringResourceType(items[0]), timeDuration, 1, MonitoringUtil.
                            getMonitoringThreshold(items[4], items[5]), MonitoringUtil.getMonitoringThreshold(
                            items[2], items[3]));
                } else if (items.length == 5) {  //ie: CPU:>=:70:<=:80
                    tablespoonAPI.createTopic(monitoringListener, groupId, EventType.REGULAR, MonitoringUtil.
                            getMonitoringResourceType(items[0]), timeDuration, 1, MonitoringUtil.getMonitoringThreshold(
                            items[3], items[4]), MonitoringUtil.getMonitoringThreshold(items[1], items[2]));
                }
            }catch(ThresholdException e){
                log.warn("Could not add the monitoring event: " + event.getInterest() + " for group: " +
                        groupId, e);
            }
        }
    }

    public void removeInterestedEvent(String groupId, InterestedEvent events, int timeDuration) {
        //TODO remove events for a limited time
    }
}
