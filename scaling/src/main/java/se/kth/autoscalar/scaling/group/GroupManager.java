package se.kth.autoscalar.scaling.group;

import se.kth.autoscalar.common.monitoring.RuleSupport;
import se.kth.autoscalar.scaling.exceptions.AutoScalarException;
import se.kth.autoscalar.scaling.rules.Rule;

import java.sql.SQLException;
import java.util.Map;


/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public interface GroupManager{
    Group createGroup(String groupName, int minInstances, int maxInstances, int coolingTimeUp, int coolingTimeDown,
                      String[] ruleNames, Map<Group.ResourceRequirement, Integer> minResourceReq, float reliabilityReq) throws AutoScalarException;

    boolean isGroupExists(String groupName) throws AutoScalarException;

    Group getGroup(String groupName) throws AutoScalarException;

    void addRuleToGroup(String groupName, String ruleName) throws AutoScalarException;

    void updateGroup(String groupName, Group group) throws AutoScalarException;

    void removeRuleFromGroup(String groupName, String ruleName) throws AutoScalarException;

    String[] getRulesForGroup(String groupName) throws AutoScalarException;

    Rule[] getMatchingRulesForGroup(String groupName, RuleSupport.ResourceType resourceType,
                                           RuleSupport.Comparator comparator, float currentValue) throws AutoScalarException;

    void deleteGroup(String groupName) throws AutoScalarException;

    void deleteTables() throws SQLException;
}
