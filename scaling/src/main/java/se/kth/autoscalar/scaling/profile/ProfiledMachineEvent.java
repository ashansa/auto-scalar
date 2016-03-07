package se.kth.autoscalar.scaling.profile;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class ProfiledMachineEvent implements ProfiledEvent {

    private String machineId;
    private String groupId;

    public ProfiledMachineEvent(String machineId) {
        this.machineId = machineId;
    }
}
