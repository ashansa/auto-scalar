package se.kth.autoscalar.scaling.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.autoscalar.common.models.MachineInfo;
import se.kth.autoscalar.common.monitoring.MonitoringEvent;
import se.kth.autoscalar.common.monitoring.RuleSupport;
import se.kth.autoscalar.scaling.MonitoringListener;
import se.kth.autoscalar.scaling.ScalingSuggestion;
import se.kth.autoscalar.scaling.exceptions.ElasticScalarException;
import se.kth.autoscalar.scaling.group.Group;
import se.kth.autoscalar.scaling.group.GroupManager;
import se.kth.autoscalar.scaling.group.GroupManagerImpl;
import se.kth.autoscalar.scaling.rules.Rule;
import se.kth.autoscalar.scaling.rules.RuleManager;
import se.kth.autoscalar.scaling.rules.RuleManagerImpl;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class ElasticScalarAPI {

    Log log = LogFactory.getLog(ElasticScalarAPI.class);

    ElasticScalingManager elasticScalingManager;
    RuleManager ruleManager;
    GroupManager groupManager;

    public ElasticScalarAPI() throws ElasticScalarException {
        elasticScalingManager = new ElasticScalingManager();
        ruleManager = RuleManagerImpl.getInstance();
        groupManager = GroupManagerImpl.getInstance();
    }

    public Rule createRule(String ruleName, RuleSupport.ResourceType resourceType, RuleSupport.Comparator comparator, float thresholdPercentage, int operationAction) throws ElasticScalarException {
        return ruleManager.createRule(ruleName, resourceType, comparator, thresholdPercentage, operationAction);
    }

    public Rule getRule(String ruleName) throws ElasticScalarException {
        return ruleManager.getRule(ruleName);
    }

    public void updateRule(String ruleName, Rule rule) throws ElasticScalarException {
        ruleManager.updateRule(ruleName, rule);
    }

    public void deleteRule(String ruleName) throws ElasticScalarException {
        ruleManager.deleteRule(ruleName);
    }

    public boolean isRuleExists(String ruleName) throws ElasticScalarException {
        return ruleManager.isRuleExists(ruleName);
    }

    public boolean isRuleInUse(String ruleName) throws ElasticScalarException {
        return ruleManager.isRuleExists(ruleName);
    }

    public String[] getRuleUsage(String ruleName) throws ElasticScalarException {
        return ruleManager.getRuleUsage(ruleName);
    }

    public Group createGroup(String groupName, int minInstances, int maxInstances, int coolingTimeUp, int coolingTimeDown,
                             String[] ruleNames, Map<Group.ResourceRequirement, Integer> minResourceReq, float reliabilityReq)
            throws ElasticScalarException {

        if (isGroupExists(groupName)) {
            String errorMsg = "A group already exists with name " + groupName + " . Group name should be unique";
            log.error(errorMsg);
            throw new ElasticScalarException(errorMsg);
        }
        return groupManager.createGroup(groupName, minInstances, maxInstances, coolingTimeUp, coolingTimeDown, ruleNames,
                minResourceReq, reliabilityReq);
    }

    public boolean isGroupExists(String groupName) throws ElasticScalarException {
        return groupManager.isGroupExists(groupName);
    }

    public Group getGroup(String groupName) throws ElasticScalarException {
        return groupManager.getGroup(groupName);
    }

    public void addRuleToGroup(String groupName, String ruleName) throws ElasticScalarException {
        groupManager.addRuleToGroup(groupName, ruleName);
    }

    //The list of rules in the group will not be updated with this method
    public void updateGroup(String groupName, Group group) throws ElasticScalarException {
        groupManager.updateGroup(groupName, group);
    }

    public void removeRuleFromGroup(String groupName, String ruleName) throws ElasticScalarException {
        groupManager.removeRuleFromGroup(groupName, ruleName);
    }

    public void deleteGroup(String groupName) throws ElasticScalarException {
        groupManager.deleteGroup(groupName);
    }

    public MachineInfo addMachineToGroup(String groupId, String machineId, String sshKeyPath, String IP, int sshPort, String userName) {
        throw new UnsupportedOperationException("#addVMToGroup()");
    }

    public void removeMachineFromGroup(MachineInfo model) {
        throw new UnsupportedOperationException("#removeMachineFromGroup()");
    }

    public MonitoringListener startElasticScaling(String groupId, int currentNumberOfMachines) throws ElasticScalarException {
        elasticScalingManager.addGroupForScaling(groupId, currentNumberOfMachines);
        return elasticScalingManager.monitoringListener;
    }

    public void stopElasticScaling(String groupId) {
        elasticScalingManager.removeGroupFromScaling(groupId);
    }

    public ArrayBlockingQueue<ScalingSuggestion> getSuggestionQueue(String groupId) {
        return elasticScalingManager.getSuggestions(groupId);
    }

    public void handleEvent(String groupId, MonitoringEvent monitoringEvent) throws ElasticScalarException {
        elasticScalingManager.getEventProfiler().profileEvent(groupId, monitoringEvent);
    }
}
