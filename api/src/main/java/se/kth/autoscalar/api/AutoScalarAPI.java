package se.kth.autoscalar.api;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.autoscalar.common.models.MachineInfo;
import se.kth.autoscalar.common.monitoring.RuleSupport.Comparator;
import se.kth.autoscalar.common.monitoring.RuleSupport.ResourceType;
import se.kth.autoscalar.scaling.MonitoringListener;
import se.kth.autoscalar.scaling.core.ElasticScalarAPI;
import se.kth.autoscalar.scaling.exceptions.ElasticScalarException;
import se.kth.autoscalar.scaling.group.Group;
import se.kth.autoscalar.scaling.rules.Rule;

import java.util.Queue;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class AutoScalarAPI {

    Log log = LogFactory.getLog(AutoScalarAPI.class);

    ElasticScalarAPI elasticScalar;

    public AutoScalarAPI() throws ElasticScalarException {
        try {
            elasticScalar = new ElasticScalarAPI();
        } catch (ElasticScalarException e) {
            log.error("Error while initiating ElasticScalarAPI. " + e.getMessage());
            throw e;
        }
    }

    public Rule createRule(String ruleName, ResourceType resourceType, Comparator comparator, float thresholdPercentage, int operationAction) throws ElasticScalarException {
        throw new UnsupportedOperationException("#createRule()");
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

    public void startAutoScaling(String groupId, int currentNumberOfMachines) throws ElasticScalarException, se.kth.autoscalar.exceptions.AutoScalarException {

        Group group = elasticScalar.getGroup(groupId);
        if (group == null) {
            String errorMsg = "Could not find a group with id " + groupId + " . A group should be created with the " +
                    "required requirements and scaling rules before starting auto scaling on a group";
            throw new se.kth.autoscalar.exceptions.AutoScalarException(errorMsg);
        }

        String[] rulesOfGroup = group.getRuleNames();
        if (rulesOfGroup.length == 0) {
            String errorMsg = "No rules have been specified to the scaling group with id " + groupId + " . Rules for the" +
                    "group should be specified before starting auto scaling on the group.";
            throw new se.kth.autoscalar.exceptions.AutoScalarException(errorMsg);
        }

        //TODO create RuleBase

        try {
            MonitoringListener monitoringListener = elasticScalar.startElasticScaling(groupId, currentNumberOfMachines);
        } catch (ElasticScalarException e) {
            log.error("Error while starting elastic scaling for group " + groupId);
            throw e;
        }

        //TODO start monitoring the group and pass monitoringListener object to monitoring module to notify

    }

    public boolean stopAutoScaling(String groupId) {
        throw new UnsupportedOperationException("#stopAutoScaling()");
    }

    public Queue getSuggestionQueue() {
        throw new UnsupportedOperationException("#getSuggestionQueue()");
    }
}
