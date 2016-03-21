package se.kth.autoscalar.scaling.profile;

import se.kth.autoscalar.common.monitoring.MachineMonitoringEvent;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class ProfiledMachineEvent implements ProfiledEvent {

    private MachineMonitoringEvent[] machineMonitoringEvents;
    private ProfiledResourceEvent profiledResourceEvent;
    private String groupId;

    public ProfiledMachineEvent(String groupId, MachineMonitoringEvent[] machineMonitoringEvents, ProfiledResourceEvent profiledResourceEvent) {
        this.groupId = groupId;
        this.machineMonitoringEvents = machineMonitoringEvents;
        this.profiledResourceEvent = profiledResourceEvent;
    }

    public String getGroupId() {
        return groupId;
    }

    public MachineMonitoringEvent[] getMachineMonitoringEvents() {
        return machineMonitoringEvents;
    }

    public ProfiledResourceEvent getProfiledResourceEvent() {
        return profiledResourceEvent;
    }
}
