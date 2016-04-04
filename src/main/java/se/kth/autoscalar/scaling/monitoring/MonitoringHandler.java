package se.kth.autoscalar.scaling.monitoring;

import se.kth.autoscalar.scaling.core.AutoScalarAPI;
import se.kth.autoscalar.scaling.exceptions.AutoScalarException;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class MonitoringHandler {

    private AutoScalarAPI autoScalarAPI;

    public MonitoringHandler(AutoScalarAPI autoScalarAPI) {
        this.autoScalarAPI = autoScalarAPI;
    }

    public MonitoringListener addGroupForMonitoring(String groupId, InterestedEvent[] interestedEvents) throws AutoScalarException {
        MonitoringListener monitoringListener = new MonitoringListener(autoScalarAPI, interestedEvents);
        //TODO call Monitoring component and give (groupId, listener)
        //TODO temporary returning  MonitoringListener to emulate monitoring events by tests
        return monitoringListener;
    }

    public void addInterestedEvent(InterestedEvent event) {

    }

    public void removeInterestedEvent(InterestedEvent event) {

    }
}
