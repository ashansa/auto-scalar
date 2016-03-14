package se.kth.autoscalar.scaling.group;

import se.kth.autoscalar.common.monitoring.RuleSupport;
import se.kth.autoscalar.scaling.exceptions.ElasticScalarException;
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
                      String[] ruleNames, Map<Group.ResourceRequirement, Integer> minResourceReq, float reliabilityReq) throws ElasticScalarException;

    boolean isGroupExists(String groupName) throws ElasticScalarException;

    Group getGroup(String groupName) throws ElasticScalarException;

    void addRuleToGroup(String groupName, String ruleName) throws ElasticScalarException;

    void updateGroup(String groupName, Group group) throws ElasticScalarException;

    void removeRuleFromGroup(String groupName, String ruleName) throws ElasticScalarException;

    String[] getRulesForGroup(String groupName) throws ElasticScalarException;

    Rule[] getMatchingRulesForGroup(String groupName, RuleSupport.ResourceType resourceType,
                                           RuleSupport.Comparator comparator, float currentValue) throws ElasticScalarException;

    void deleteGroup(String groupName) throws ElasticScalarException;

    void deleteTables() throws SQLException;
}
