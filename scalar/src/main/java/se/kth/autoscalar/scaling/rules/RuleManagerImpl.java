package se.kth.autoscalar.scaling.rules;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.autoscalar.common.monitoring.RuleSupport.Comparator;
import se.kth.autoscalar.common.monitoring.RuleSupport.ResourceType;
import se.kth.autoscalar.scaling.exceptions.DBConnectionFailureException;
import se.kth.autoscalar.scaling.exceptions.ElasticScalarException;
import se.kth.autoscalar.scaling.group.GroupDAO;

import java.sql.SQLException;


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

    public static RuleManagerImpl getInstance() throws DBConnectionFailureException {
        if(ruleManager == null) {
            ruleManager  = new RuleManagerImpl();
            ruleManager.init();
        }
        return ruleManager;
    }

    private void init() throws DBConnectionFailureException {
        ruleDAO = RuleDAO.getInstance();
    }

    public Rule createRule(String ruleName, ResourceType resourceType, Comparator comparator,
                           float thresholdPercentage, int operationAction) throws ElasticScalarException {
        try {
            Rule rule = new Rule(ruleName, resourceType, comparator, thresholdPercentage,
                    operationAction);
            ruleDAO.createRule(rule);
            return rule;
        } catch (SQLException e) {
            throw new ElasticScalarException("Failed to create rule. Name: " + ruleName, e.getCause());
        }
    }

    public Rule getRule(String ruleName) throws ElasticScalarException {
        try {
            return ruleDAO.getRule(ruleName);
        } catch (SQLException e) {
            throw new ElasticScalarException("Failed to get rule with name " + ruleName, e.getCause());
        }
    }

    public void updateRule(String ruleName, Rule rule) throws ElasticScalarException {
        try {
            ruleDAO.updateRule(ruleName, rule);
        } catch (SQLException e) {
            throw new ElasticScalarException("Failed to update rule. Name: " + ruleName, e.getCause());
        }
    }

    public void deleteRule(String ruleName) throws ElasticScalarException {
        try {
            ruleDAO.deleteRule(ruleName);
        } catch (SQLException e) {
            throw new ElasticScalarException("Failed to delete rule. Name: " + ruleName, e.getCause());
        }
    }

    public boolean isRuleExists(String ruleName) throws ElasticScalarException {
        try {
            return ruleDAO.isRuleAlreadyExists(ruleName);
        } catch (SQLException e) {
            throw new ElasticScalarException("Failed to check whether rule exists for rule " + ruleName, e.getCause());
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
    public String[] getRuleUsage(String ruleName) throws ElasticScalarException {
        try {
            return GroupDAO.getInstance().getGroupNamesForRule(ruleName);
        } catch (DBConnectionFailureException e) {
            throw new ElasticScalarException("Failed to get the usage for rule " + ruleName, e.getCause());
        } catch (SQLException e) {
            throw new ElasticScalarException("Failed to get the usage for rule " + ruleName, e.getCause());
        }
    }

}