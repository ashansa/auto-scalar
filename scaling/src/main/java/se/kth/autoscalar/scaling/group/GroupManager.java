package se.kth.autoscalar.scaling.group;

import se.kth.autoscalar.common.models.MachineInfo;
import se.kth.autoscalar.common.monitoring.RuleSupport;
import se.kth.autoscalar.scaling.exceptions.ElasticScalarException;
import se.kth.autoscalar.scaling.rules.Rule;


/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public interface GroupManager{
    Group createGroup(String groupName, int minInstances, int maxInstances, int coolingTimeUp, int coolingTimeDown,
                      String[] ruleNames) throws ElasticScalarException;

    boolean isGroupExists(String groupName) throws ElasticScalarException;

    Group getGroup(String groupName) throws ElasticScalarException;

    void addRuleToGroup(String groupName, String ruleName) throws ElasticScalarException;

    void updateGroup(String groupName, Group group) throws ElasticScalarException;

    void removeRuleFromGroup(String groupName, String ruleName) throws ElasticScalarException;

    String[] getRulesForGroup(String groupName) throws ElasticScalarException;

    public Rule[] getMatchingRulesForGroup(String groupName, RuleSupport.ResourceType resourceType,
                                           RuleSupport.Comparator comparator, float currentValue) throws ElasticScalarException;

    void deleteGroup(String groupName) throws ElasticScalarException;

    void addMachineToGroup(MachineInfo machineInfo);

    void removeMachineFromGroup(MachineInfo model);
}
