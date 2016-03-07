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

    private Status status;
    private String machineId;

    public Status getStatus() {
        return status;
    }

    public String getMachineId() {
        return machineId;
    }
}
