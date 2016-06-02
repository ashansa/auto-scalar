package se.kth.honeytap.scaling.rules;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.honeytap.scaling.exceptions.DBConnectionFailureException;
import se.kth.honeytap.scaling.exceptions.HoneyTapException;
import se.kth.honeytap.scaling.group.GroupDAO;
import se.kth.honeytap.scaling.monitoring.RuleSupport;

import java.sql.SQLException;
import java.util.ArrayList;


/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class RuleManagerImpl implements RuleManager {

    Log log = LogFactory.getLog(RuleManagerImpl.class);

    private static RuleManagerImpl ruleManager;
    private RuleDAO ruleDAO;

    private RuleManagerImpl() { }

    public static RuleManagerImpl getInstance() throws HoneyTapException {
        try {
            if (ruleManager == null) {
                ruleManager = new RuleManagerImpl();
                ruleManager.init();
            }
            return ruleManager;
        } catch (DBConnectionFailureException e) {
            throw new HoneyTapException(e.getMessage(), e);
        }
    }

    private void init() throws DBConnectionFailureException {
        ruleDAO = RuleDAO.getInstance();
    }

    public Rule createRule(String ruleName, RuleSupport.ResourceType resourceType, RuleSupport.Comparator comparator,
                           float thresholdPercentage, int operationAction) throws HoneyTapException {
        try {
            Rule rule = new Rule(ruleName, resourceType, comparator, thresholdPercentage,
                    operationAction);
            ruleDAO.createRule(rule);
            return rule;
        } catch (SQLException e) {
            throw new HoneyTapException("Failed to create rule. Name: " + ruleName, e.getCause());
        }
    }

    public Rule getRule(String ruleName) throws HoneyTapException {
        try {
            return ruleDAO.getRule(ruleName);
        } catch (SQLException e) {
            throw new HoneyTapException("Failed to get rule with name " + ruleName, e.getCause());
        }
    }

    public void updateRule(String ruleName, Rule rule) throws HoneyTapException {
        try {
            ruleDAO.updateRule(ruleName, rule);
        } catch (SQLException e) {
            throw new HoneyTapException("Failed to update rule. Name: " + ruleName, e.getCause());
        }
    }

    public void deleteRule(String ruleName) throws HoneyTapException {
        try {
            ruleDAO.deleteRule(ruleName);
        } catch (SQLException e) {
            throw new HoneyTapException("Failed to delete rule. Name: " + ruleName, e.getCause());
        }
    }

    public boolean isRuleExists(String ruleName) throws HoneyTapException {
        try {
            return ruleDAO.isRuleAlreadyExists(ruleName);
        } catch (SQLException e) {
            throw new HoneyTapException("Failed to check whether rule exists for rule " + ruleName, e.getCause());
        }
    }

    public boolean isRuleInUse(String ruleName) {
        return ruleDAO.getRuleUsage(ruleName).length > 0 ? true : false;
    }

    /**
     *
     * @param ruleName
     * @return the list of group names of which the rule is being used
     */
    public String[] getRuleUsage(String ruleName) throws HoneyTapException {
        try {
            return GroupDAO.getInstance().getGroupNamesForRule(ruleName);
        } catch (DBConnectionFailureException e) {
            throw new HoneyTapException("Failed to get the usage for rule " + ruleName, e.getCause());
        } catch (SQLException e) {
            throw new HoneyTapException("Failed to get the usage for rule " + ruleName, e.getCause());
        }
    }

    public Rule[] getMatchingRulesForConstraints(String[] ruleNames, RuleSupport.ResourceType eventResourceType, RuleSupport.Comparator eventComparator, float eventValue) {
        ArrayList<Rule> matchingRules = new ArrayList<Rule>();
        for (String ruleName : ruleNames) {
            try {
                Rule rule = getRule(ruleName);
                if (rule == null) {
                    log.error("Could not find the rule for rule name: " + ruleName);
                } else {
                    if (rule.getResourceType().name().equals(eventResourceType.name())) {
                        RuleSupport.Comparator ruleComparator = rule.getComparator();
                        if (RuleSupport.Comparator.GREATER_THAN.equals(eventComparator) || RuleSupport.Comparator.GREATER_THAN_OR_EQUAL.equals(eventComparator)) {
                            if (RuleSupport.Comparator.GREATER_THAN.equals(ruleComparator)) {
                                if (eventValue > rule.getThreshold())
                                    matchingRules.add(rule);
                            } else if (RuleSupport.Comparator.GREATER_THAN_OR_EQUAL.equals(ruleComparator)) {
                                if (eventValue >= rule.getThreshold())
                                    matchingRules.add(rule);
                            }
                        } else if (RuleSupport.Comparator.LESS_THAN.equals(eventComparator) || RuleSupport.Comparator.LESS_THAN_OR_EQUAL.equals(eventComparator)) {
                            if (RuleSupport.Comparator.LESS_THAN.equals(ruleComparator)) {
                                if (eventValue < rule.getThreshold())
                                    matchingRules.add(rule);
                            } else if (RuleSupport.Comparator.LESS_THAN_OR_EQUAL.equals(ruleComparator)) {
                                if (eventValue <= rule.getThreshold())
                                    matchingRules.add(rule);
                            }
                        }
                    }
                }
            } catch (HoneyTapException e) {
                log.error("Error while retrieving the rule for name " + ruleName);
            }
        }
        return matchingRules.toArray(new Rule[matchingRules.size()]);
    }

    public void deleteTables() throws SQLException {
        ruleDAO.tempMethodDeleteTable();
    }
}