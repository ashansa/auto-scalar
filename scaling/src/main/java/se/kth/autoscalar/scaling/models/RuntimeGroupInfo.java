package se.kth.autoscalar.scaling.models;

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

    public void setLastScaleInTime(Date lastScaleInTime) {
        this.lastScaleInTime = lastScaleInTime;
    }

    public Date getLastScaleOutTime() {
        return lastScaleOutTime;
    }

    public void setLastScaleOutTime(Date lastScaleOutTime) {
        this.lastScaleOutTime = lastScaleOutTime;
    }

    public int getNumberOfMachinesInGroup() {
        return numberOfMachinesInGroup;
    }

    public synchronized void incrementNoOfMachinesInGroup(int number) {
        numberOfMachinesInGroup += number;
    }

    public synchronized void decrementNoOfMachinesInGroup(int number) {
        numberOfMachinesInGroup -= number;
    }

}
