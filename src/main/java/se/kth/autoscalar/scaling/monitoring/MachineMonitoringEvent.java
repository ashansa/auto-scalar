package se.kth.autoscalar.scaling.monitoring;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */

/**
 * Events on 'close to billing period - decide on shutting down'
 * Events on machine failures
 */
public class MachineMonitoringEvent extends MonitoringEvent {

    public enum Status {
        KILLED, AT_END_OF_BILLING_PERIOD
    }

    private Status status;

    public MachineMonitoringEvent(String groupId, String machineId, Status status) {
        super(groupId, machineId);
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }
}
