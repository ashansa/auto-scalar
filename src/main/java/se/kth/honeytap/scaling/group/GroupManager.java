package se.kth.honeytap.scaling.group;

import se.kth.honeytap.scaling.exceptions.HoneyTapException;
import se.kth.honeytap.scaling.monitoring.RuleSupport;
import se.kth.honeytap.scaling.rules.Rule;

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
                      String[] ruleNames, Map<Group.ResourceRequirement, Integer> minResourceReq, float reliabilityReq) throws HoneyTapException;

    boolean isGroupExists(String groupName) throws HoneyTapException;

    Group getGroup(String groupName) throws HoneyTapException;

    void addRuleToGroup(String groupName, String ruleName) throws HoneyTapException;

    void updateGroup(String groupName, Group group) throws HoneyTapException;

    void removeRuleFromGroup(String groupName, String ruleName) throws HoneyTapException;

    String[] getRulesForGroup(String groupName) throws HoneyTapException;

    Rule[] getMatchingRulesForGroup(String groupName, RuleSupport.ResourceType resourceType,
                                           RuleSupport.Comparator comparator, float currentValue) throws HoneyTapException;

    void deleteGroup(String groupName) throws HoneyTapException;

    void deleteTables() throws SQLException;
}
