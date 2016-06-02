package se.kth.honeytap.scaling.rules;

import se.kth.honeytap.scaling.exceptions.HoneyTapException;
import se.kth.honeytap.scaling.monitoring.RuleSupport;

import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public interface RuleManager {

    Rule createRule(String ruleName, RuleSupport.ResourceType resourceType, RuleSupport.Comparator comparator,
                    float thresholdPercentage, int operationAction) throws HoneyTapException;

    Rule getRule(String ruleName) throws HoneyTapException;

    void updateRule(String ruleName, Rule rule) throws HoneyTapException;

    void deleteRule(String ruleName) throws HoneyTapException;

    boolean isRuleExists(String ruleName) throws HoneyTapException;

    boolean isRuleInUse(String ruleName);

    String[] getRuleUsage(String ruleName) throws HoneyTapException;

    Rule[] getMatchingRulesForConstraints(String[] ruleNames, RuleSupport.ResourceType eventResourceType, RuleSupport.Comparator eventComparator, float currentValue);

    void deleteTables() throws SQLException;
}
