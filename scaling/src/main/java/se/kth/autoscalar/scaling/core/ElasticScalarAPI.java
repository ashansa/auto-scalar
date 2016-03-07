package se.kth.autoscalar.scaling.core;

import se.kth.autoscalar.common.models.MachineInfo;
import se.kth.autoscalar.common.monitoring.RuleSupport;
import se.kth.autoscalar.scaling.MonitoringListener;
import se.kth.autoscalar.scaling.exceptions.ElasticScalarException;
import se.kth.autoscalar.scaling.group.Group;
import se.kth.autoscalar.scaling.group.GroupManager;
import se.kth.autoscalar.scaling.group.GroupManagerImpl;
import se.kth.autoscalar.scaling.rules.Rule;
import se.kth.autoscalar.scaling.rules.RuleManager;
import se.kth.autoscalar.scaling.rules.RuleManagerImpl;

import java.util.Queue;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class ElasticScalarAPI {

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
        throw new UnsupportedOperationException("#getRule()");
    }

    public void updateRule(String ruleName, Rule rule) throws ElasticScalarException {
        throw new UnsupportedOperationException("#updateRule()");
    }

    public void deleteRule(String ruleName) throws ElasticScalarException {
        throw new UnsupportedOperationException("#deleteRule()");
    }

    public boolean isRuleExists(String ruleName) throws ElasticScalarException {
        throw new UnsupportedOperationException("#isRuleExists()");
    }

    public boolean isRuleInUse(String ruleName) {
        throw new UnsupportedOperationException("#isRuleInUse()");
    }

    public String[] getRuleUsage(String ruleName) throws ElasticScalarException {
        throw new UnsupportedOperationException("#getRuleUsage()");
    }

    public Group createGroup(String groupName, int minInstances, int maxInstances, int coolingTimeUp, int coolingTimeDown, String[] ruleNames) throws ElasticScalarException {
        throw new UnsupportedOperationException("#createGroup()");
    }

    public boolean isGroupExists(String groupName) throws ElasticScalarException {
        throw new UnsupportedOperationException("#isGroupExists()");
    }

    public Group getGroup(String groupName) throws ElasticScalarException {
        throw new UnsupportedOperationException("#getGroup()");
    }

    public void addRuleToGroup(String groupName, String ruleName) throws ElasticScalarException {
        throw new UnsupportedOperationException("#addRuleToGroup()");
    }

    public void updateGroup(String groupName, Group group) throws ElasticScalarException {
        throw new UnsupportedOperationException("#updateGroup()");
    }

    public void removeRuleFromGroup(String groupName, String ruleName) throws ElasticScalarException {
        throw new UnsupportedOperationException("#removeRuleFromGroup()");
    }

    public void deleteGroup(String groupName) throws ElasticScalarException {
        throw new UnsupportedOperationException("#deleteGroup()");
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

    public boolean stopElasticScaling(String groupId) {
        throw new UnsupportedOperationException("#stopElasticScaling()");
    }

    public Queue getSuggestionQueue() {
        throw new UnsupportedOperationException("#getSuggestionQueue()");
    }
}
