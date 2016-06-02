package se.kth.honeytap.scaling.models;

import java.util.Calendar;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class RuntimeGroupInfo {

    private String groupId;
    private int numberOfMachinesInGroup;
    private Date lastScaleInTime;
    private Date lastScaleOutTime;

    public RuntimeGroupInfo(String groupId, int numberOfMachines) {
        this.groupId = groupId;
        this.numberOfMachinesInGroup = numberOfMachines;
    }

    public Date getLastScaleInTime() {
        return lastScaleInTime;
    }

    public Date getLastScaleOutTime() {
        return lastScaleOutTime;
    }

    public int getNumberOfMachinesInGroup() {
        return numberOfMachinesInGroup;
    }

    public synchronized void setScaleOutInfo(int numberOfMachines) {
        numberOfMachinesInGroup += numberOfMachines;
        lastScaleOutTime = Calendar.getInstance().getTime();
    }

    public synchronized void setScaleInInfo(int numberOfMachines) {
        numberOfMachinesInGroup -= numberOfMachines;
        lastScaleInTime = Calendar.getInstance().getTime();
    }

}
