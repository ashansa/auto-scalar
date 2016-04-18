package se.kth.autoscalar.scaling.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.autoscalar.scaling.ScalingSuggestion;
import se.kth.autoscalar.scaling.exceptions.AutoScalarException;
import se.kth.autoscalar.scaling.group.Group;
import se.kth.autoscalar.scaling.group.GroupManager;
import se.kth.autoscalar.scaling.group.GroupManagerImpl;
import se.kth.autoscalar.scaling.models.MachineInfo;
import se.kth.autoscalar.scaling.monitoring.MonitoringEvent;
import se.kth.autoscalar.scaling.monitoring.MonitoringHandler;
import se.kth.autoscalar.scaling.monitoring.MonitoringListener;
import se.kth.autoscalar.scaling.monitoring.RuleSupport;
import se.kth.autoscalar.scaling.profile.ProfiledResourceEvent;
import se.kth.autoscalar.scaling.rules.Rule;
import se.kth.autoscalar.scaling.rules.RuleManager;
import se.kth.autoscalar.scaling.rules.RuleManagerImpl;

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
public class AutoScalarAPI {

    Log log = LogFactory.getLog(AutoScalarAPI.class);

    private AutoScalingManager autoScalingManager;
    private RuleManager ruleManager;
    private GroupManager groupManager;
    private static AutoScalarAPI autoScalarAPI;

    private AutoScalarAPI() throws AutoScalarException {
        MonitoringHandler monitoringHandler = new MonitoringHandler(this);
        autoScalingManager = new AutoScalingManager(monitoringHandler);
        ruleManager = RuleManagerImpl.getInstance();
        groupManager = GroupManagerImpl.getInstance();
    }

    public static AutoScalarAPI getInstance() throws AutoScalarException {
        try {
            if (autoScalarAPI == null) {
                autoScalarAPI = new AutoScalarAPI();
            }
            return autoScalarAPI;
        } catch (AutoScalarException e) {
            throw new AutoScalarException("Error while initializing AutoScalarAPI", e);
        }
    }

    public Rule createRule(String ruleName, RuleSupport.ResourceType resourceType, RuleSupport.Comparator comparator, float thresholdPercentage, int operationAction) throws AutoScalarException {
        return ruleManager.createRule(ruleName, resourceType, comparator, thresholdPercentage, operationAction);
    }

    public Rule getRule(String ruleName) throws AutoScalarException {
        return ruleManager.getRule(ruleName);
    }

    public void updateRule(String ruleName, Rule rule) throws AutoScalarException {
        ruleManager.updateRule(ruleName, rule);
    }

    public void deleteRule(String ruleName) throws AutoScalarException {
        ruleManager.deleteRule(ruleName);
    }

    public boolean isRuleExists(String ruleName) throws AutoScalarException {
        return ruleManager.isRuleExists(ruleName);
    }

    public boolean isRuleInUse(String ruleName) throws AutoScalarException {
        return ruleManager.isRuleExists(ruleName);
    }

    public String[] getRuleUsage(String ruleName) throws AutoScalarException {
        return ruleManager.getRuleUsage(ruleName);
    }

    public Group createGroup(String groupName, int minInstances, int maxInstances, int coolingTimeUp, int coolingTimeDown,
                             String[] ruleNames, Map<Group.ResourceRequirement, Integer> minResourceReq, float reliabilityReq)
            throws AutoScalarException {

        if (isGroupExists(groupName)) {
            String errorMsg = "A group already exists with name " + groupName + " . Group name should be unique";
            log.error(errorMsg);
            throw new AutoScalarException(errorMsg);
        }
        return groupManager.createGroup(groupName, minInstances, maxInstances, coolingTimeUp, coolingTimeDown, ruleNames,
                minResourceReq, reliabilityReq);
    }

    public boolean isGroupExists(String groupName) throws AutoScalarException {
        return groupManager.isGroupExists(groupName);
    }

    public Group getGroup(String groupName) throws AutoScalarException {
        return groupManager.getGroup(groupName);
    }

    public void addRuleToGroup(String groupName, String ruleName) throws AutoScalarException {
        groupManager.addRuleToGroup(groupName, ruleName);
    }

    //The list of rules in the group will not be updated with this method
    public void updateGroup(String groupName, Group group) throws AutoScalarException {
        groupManager.updateGroup(groupName, group);
    }

    public void removeRuleFromGroup(String groupName, String ruleName) throws AutoScalarException {
        groupManager.removeRuleFromGroup(groupName, ruleName);
    }

    public void deleteGroup(String groupName) throws AutoScalarException {
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

    public MonitoringListener startAutoScaling(String groupId, int currentNumberOfMachines) throws AutoScalarException {
        return autoScalingManager.startAutoScaling(groupId, currentNumberOfMachines);
    }

    public void stopElasticScaling(String groupId) {
        autoScalingManager.stopAutoScaling(groupId);
    }

    public ArrayBlockingQueue<ScalingSuggestion> getSuggestionQueue(String groupId) {
        return autoScalingManager.getSuggestions(groupId);
    }

    public void handleEvent(String groupId, MonitoringEvent monitoringEvent) throws AutoScalarException {
        autoScalingManager.getEventProfiler().profileEvent(groupId, monitoringEvent);
    }

    public int getNumberOfMachineChanges(ProfiledResourceEvent event) throws AutoScalarException {
        return autoScalingManager.getNumberOfMachineChanges(event);
    }

    public void tempMethodDeleteTables() throws SQLException {
        ruleManager.deleteTables();
        groupManager.deleteTables();
    }
}
