package se.kth.autoscalar.common.monitoring;

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

    private String groupId;
    private String machineId;
    private Status status;

    public MachineMonitoringEvent(String groupId, String machineId, Status status) {
        this.groupId = groupId;
        this.machineId = machineId;
        this.status = status;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getMachineId() {
        return machineId;
    }

    public Status getStatus() {
        return status;
    }
}
