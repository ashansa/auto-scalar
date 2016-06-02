package se.kth.honeytap.scaling.monitoring;

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
    private long timeRemaining = 0;

    public MachineMonitoringEvent(String groupId, String machineId, Status status) {
        super(groupId, machineId);
        this.status = status;
    }

    public long getTimeRemaining() {
        return timeRemaining;
    }

  /**
   * Set time remaining to end of billing period in milliseconds
   * @param timeRemaining
   */
  public void setTimeRemaining(long timeRemaining) {
        this.timeRemaining = timeRemaining;
    }

    public Status getStatus() {
        return status;
    }
}
