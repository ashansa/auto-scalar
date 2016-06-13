package se.kth.honeytap.scaling.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.honeytap.scaling.ScalingSuggestion;
import se.kth.honeytap.scaling.exceptions.HoneyTapException;
import se.kth.honeytap.scaling.group.Group;
import se.kth.honeytap.scaling.group.GroupManager;
import se.kth.honeytap.scaling.group.GroupManagerImpl;
import se.kth.honeytap.scaling.models.MachineInfo;
import se.kth.honeytap.scaling.monitoring.MonitoringEvent;
import se.kth.honeytap.scaling.monitoring.MonitoringHandler;
import se.kth.honeytap.scaling.monitoring.MonitoringHandlerSimulator;
import se.kth.honeytap.scaling.monitoring.MonitoringListener;
import se.kth.honeytap.scaling.monitoring.RuleSupport;
import se.kth.honeytap.scaling.profile.ProfiledResourceEvent;
import se.kth.honeytap.scaling.rules.Rule;
import se.kth.honeytap.scaling.rules.RuleManager;
import se.kth.honeytap.scaling.rules.RuleManagerImpl;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class HoneyTapAPI {

    Log log = LogFactory.getLog(HoneyTapAPI.class);

    private HoneyTapManager honeyTapManager;
    private RuleManager ruleManager;
    private GroupManager groupManager;
    private int faultToleranceLevel = 1;
    MonitoringHandler monitoringHandler;

    public HoneyTapAPI() throws HoneyTapException {
        /////////MonitoringHandler monitoringHandler = new TSMonitoringHandler(this);
        monitoringHandler = new MonitoringHandlerSimulator(this);
        honeyTapManager = new HoneyTapManager(monitoringHandler);
        ruleManager = RuleManagerImpl.getInstance();
        groupManager = GroupManagerImpl.getInstance();
    }

    public Rule createRule(String ruleName, RuleSupport.ResourceType resourceType, RuleSupport.Comparator comparator, float thresholdPercentage, int operationAction) throws HoneyTapException {
        return ruleManager.createRule(ruleName, resourceType, comparator, thresholdPercentage, operationAction);
    }

    public Rule getRule(String ruleName) throws HoneyTapException {
        return ruleManager.getRule(ruleName);
    }

    public void updateRule(String ruleName, Rule rule) throws HoneyTapException {
        ruleManager.updateRule(ruleName, rule);
    }

    public void deleteRule(String ruleName) throws HoneyTapException {
        ruleManager.deleteRule(ruleName);
    }

    public boolean isRuleExists(String ruleName) throws HoneyTapException {
        return ruleManager.isRuleExists(ruleName);
    }

    public boolean isRuleInUse(String ruleName) throws HoneyTapException {
        return ruleManager.isRuleExists(ruleName);
    }

    public String[] getRuleUsage(String ruleName) throws HoneyTapException {
        return ruleManager.getRuleUsage(ruleName);
    }

  /**
   *
   * @param groupName
   * @param minInstances
   * @param maxInstances
   * @param coolingTimeOut minimum time interval in seconds between two scale out actions
   * @param coolingTimeIn
   * @param ruleNames
   * @param minResourceReq
   * @param reliabilityReq
   * @return
   * @throws HoneyTapException
   */
    public Group createGroup(String groupName, int minInstances, int maxInstances, int coolingTimeOut, int coolingTimeIn,
                             String[] ruleNames, Map<Group.ResourceRequirement, Integer> minResourceReq, float reliabilityReq)
            throws HoneyTapException {

        if (isGroupExists(groupName)) {
            String errorMsg = "A group already exists with name " + groupName + " . Group name should be unique";
            log.error(errorMsg);
            throw new HoneyTapException(errorMsg);
        }
        /*if (reliabilityReq < 100) {
             minInstances = minInstances + faultToleranceLevel;
        }*/
        return groupManager.createGroup(groupName, minInstances, maxInstances, coolingTimeOut, coolingTimeIn, ruleNames,
                minResourceReq, reliabilityReq);
    }

    public boolean isGroupExists(String groupName) throws HoneyTapException {
        return groupManager.isGroupExists(groupName);
    }

    public Group getGroup(String groupName) throws HoneyTapException {
        return groupManager.getGroup(groupName);
    }

    public void addRuleToGroup(String groupName, String ruleName) throws HoneyTapException {
        groupManager.addRuleToGroup(groupName, ruleName);
    }

    //The list of rules in the group will not be updated with this method
    public void updateGroup(String groupName, Group group) throws HoneyTapException {
        groupManager.updateGroup(groupName, group);
    }

    public void removeRuleFromGroup(String groupName, String ruleName) throws HoneyTapException {
        groupManager.removeRuleFromGroup(groupName, ruleName);
    }

    public void deleteGroup(String groupName) throws HoneyTapException {
        groupManager.deleteGroup(groupName);
    }

    /*
    Since scalar work on events, ssh info is not needed
     */
    public MachineInfo addMachineToGroup(String groupId, String machineId, String sshKeyPath, String IP, int sshPort, String userName) {
        throw new UnsupportedOperationException("#addVMToGroup()");
    }

    public void removeMachineFromGroup(MachineInfo model) {
        throw new UnsupportedOperationException("#removeMachineFromGroup()");
    }

    public MonitoringListener startAutoScaling(String groupId, int currentNumberOfMachines) throws HoneyTapException {
        return honeyTapManager.startAutoScaling(groupId, currentNumberOfMachines);
    }

    public void stopElasticScaling(String groupId) {
        honeyTapManager.stopAutoScaling(groupId);
    }

    public ArrayBlockingQueue<ScalingSuggestion> getSuggestionQueue(String groupId) {
        return honeyTapManager.getSuggestions(groupId);
    }

    public void handleEvent(String groupId, MonitoringEvent monitoringEvent) throws HoneyTapException {
        honeyTapManager.getEventProfiler().profileEvent(groupId, monitoringEvent);
    }

    public int getNumberOfMachineChanges(ProfiledResourceEvent event) throws HoneyTapException {
        return honeyTapManager.getNumberOfMachineChanges(event);
    }

    public void addVmInfo(String groupId, String vmId, int numVCpu, double memInGig, Integer numDisks, Integer diskSize, boolean reset) {
        ((MonitoringHandlerSimulator) monitoringHandler).addVmInfo(groupId, vmId, numVCpu, memInGig, numDisks, diskSize, reset);
    }

    public void addSimulatedVmInfo(String groupId, String vmId, int numVCpu, double memInGig, Integer numDisks, Integer diskSize) {
        ((MonitoringHandlerSimulator) monitoringHandler).addSimulatedVMInfo(groupId, vmId, numVCpu, memInGig, numDisks, diskSize);
    }

    public void removeSimulatedVmInfo(String groupId, String vmId) {
        ((MonitoringHandlerSimulator) monitoringHandler).removeSimulatedVMInfo(groupId, vmId);
    }

    public String[] getAllSimulatedVmIds(String groupId) {
      return ((MonitoringHandlerSimulator) monitoringHandler).getAllSimulatedVmIds(groupId);
    }
    public void tempMethodDeleteTables() throws SQLException {
        ruleManager.deleteTables();
        groupManager.deleteTables();
    }
}
