package se.kth.autoscalar.scaling.rules;

import se.kth.autoscalar.scaling.monitoring.RuleSupport.Comparator;
import se.kth.autoscalar.scaling.monitoring.RuleSupport.ResourceType;
import se.kth.autoscalar.scaling.exceptions.AutoScalarException;

import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public interface RuleManager {

    Rule createRule(String ruleName, ResourceType resourceType, Comparator comparator,
                    float thresholdPercentage, int operationAction) throws AutoScalarException;

    Rule getRule(String ruleName) throws AutoScalarException;

    void updateRule(String ruleName, Rule rule) throws AutoScalarException;

    void deleteRule(String ruleName) throws AutoScalarException;

    boolean isRuleExists(String ruleName) throws AutoScalarException;

    boolean isRuleInUse(String ruleName);

    String[] getRuleUsage(String ruleName) throws AutoScalarException;

    Rule[] getMatchingRulesForConstraints(String[] ruleNames, ResourceType eventResourceType, Comparator eventComparator, float currentValue);

    void deleteTables() throws SQLException;
}
