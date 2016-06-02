package se.kth.honeytap.scaling.monitoring;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class MonitoringEvent {

    private String groupId;
    private String machineId;

    MonitoringEvent(String groupId, String machineId) {
        this.groupId = groupId;
        this.machineId = machineId;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getMachineId() {
        return machineId;
    }
}
