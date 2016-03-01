package se.kth.autoscalar.api;

import se.kth.autoscalar.common.models.MachineInfo;
import se.kth.autoscalar.common.monitoring.RuleSupport.ResourceType;
import se.kth.autoscalar.common.monitoring.RuleSupport.Comparator;
import se.kth.autoscalar.scaling.exceptions.ElasticScalarException;
import se.kth.autoscalar.scaling.group.Group;
import se.kth.autoscalar.scaling.rules.Rule;

import java.util.Queue;

;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public interface AutoScalarAPI {

    Rule createRule(String ruleName, ResourceType resourceType, Comparator comparator,
                    float thresholdPercentage, int operationAction) throws ElasticScalarException;

    Rule getRule(String ruleName) throws ElasticScalarException;

    void updateRule(String ruleName, Rule rule) throws ElasticScalarException;

    void deleteRule(String ruleName) throws ElasticScalarException;

    boolean isRuleExists(String ruleName) throws ElasticScalarException;

    boolean isRuleInUse(String ruleName);

    String[] getRuleUsage(String ruleName) throws ElasticScalarException;

    Group createGroup(String groupName, int minInstances, int maxInstances, int coolingTimeUp, int coolingTimeDown,
                      String[] ruleNames) throws ElasticScalarException;

    boolean isGroupExists(String groupName) throws ElasticScalarException;

    Group getGroup(String groupName) throws ElasticScalarException;

    void addRuleToGroup(String groupName, String ruleName) throws ElasticScalarException;

    void updateGroup(String groupName, Group group) throws ElasticScalarException;

    void removeRuleFromGroup(String groupName, String ruleName) throws ElasticScalarException;

    void deleteGroup(String groupName) throws ElasticScalarException;

    //add to group and setup monitoring (auto scalar cannot add autoscaled machines since machins are not spawned by autoscalar)
    MachineInfo addMachineToGroup(String groupId, String machineId, String sshKeyPath, String IP, int sshPort, String userName);

    void removeMachineFromGroup(MachineInfo model);

    boolean startAutoScaling(String groupId);

    boolean stopAutoScaling(String groupId);

    Queue getSuggestionQueue();
}
