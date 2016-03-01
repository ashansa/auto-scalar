package se.kth.autoscalar.monitoring.cloud;

import org.drools.RuleBase;
import se.kth.autoscalar.common.models.MachineInfo;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */



/**
 * Cloud monitor will deploy a monitoring tool on provided VMs and expose system data on provided IP:port
 */
public interface SystemMonitorManager {

    MachineInfo setupMonitoring(String groupId, String machineId, String sshKeyPath, String IP, int sshPort, String userName);    //this should be stored in
            //monitoring module so that it can do below operations just by getting VMId
            // Q- best option? 1) give groupName too here and return VMModel and pass it to ES
            //                 2) user create VMModel and give it to ES
            //                 3) user get VMModel with groupName = null, user sets groupName and give it to ES

    boolean startMonitoring(String machineId);

    //Monitoring module will execute the target for the rules (ex: call ES listener method if CPU > 80%)
    boolean addListener(String machineId, String ruleBaseId, RuleBase ruleBase);

    //previous RuleBase will be replaced by new RuleBase
    boolean updateListenerRules(String machineId, String ruleBaseId, RuleBase ruleBase);

    boolean removeListener(String machineId, String ruleBaseId);

    boolean restartMonitoring(String machineId);

    boolean stopMonitoring(String machineId);
}
