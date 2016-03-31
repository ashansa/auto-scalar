package se.kth.autoscalar.scaling.models;

import java.util.Calendar;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class MachineInfo {

    private String vmID;
    private String groupName;
    private String sshKeyPath;
    private int sshPort;

    //a socket with below two parameters will be used push the monitoring details
    private String publicId;
    private int portForSocket;  //TODO check whether we can push data to a given port
    private Date creationTime;

    public MachineInfo(String vmID, String groupName, String sshKeyPath, String publicId, int sshPort) {
        this.vmID = vmID;
        this.groupName = groupName;
        this.sshKeyPath = sshKeyPath;
        this.publicId = publicId;
        this.sshPort = sshPort;
        setMachineCreationTime();
    }

    public String getVmID() {
        return vmID;
    }

    public String getGroupName() {
        return groupName;
    }

    public String getPublicId() {
        return publicId;
    }

    public int getSshPort() {
        return sshPort;
    }

    public String getSshKeyPath() {
        return sshKeyPath;
    }

    public Date getCreationTime() {
        return creationTime;
    }

    private void setMachineCreationTime() {
        /*java.util.TimeZone tz = java.util.TimeZone.getTimeZone("GMT+1");
        java.util.Calendar c = java.util.Calendar.getInstance(tz);

        System.out.println(c.get(java.util.Calendar.HOUR_OF_DAY)+":"+c.get(java.util.Calendar.MINUTE)+":"+c.get(java.util.Calendar.SECOND));*/

        creationTime = Calendar.getInstance().getTime();
    }
}
