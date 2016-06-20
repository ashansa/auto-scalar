package se.kth.honeytap.scaling;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import se.kth.honeytap.scaling.exceptions.DBConnectionFailureException;
import se.kth.honeytap.scaling.exceptions.HoneyTapException;
import se.kth.honeytap.scaling.rules.Rule;
import se.kth.honeytap.scaling.rules.RuleManager;
import se.kth.honeytap.scaling.rules.RuleManagerImpl;
import se.kth.honeytap.scaling.monitoring.RuleSupport;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class RuleTestIT {

    private static final String ruleNameBase = "testRule";
    private static final RuleSupport.ResourceType cpu = RuleSupport.ResourceType.CPU;
    private static final RuleSupport.Comparator greaterThan = RuleSupport.Comparator.GREATER_THAN;
    private static RuleManager ruleManager;

    @BeforeClass
    public static void setUp() throws HoneyTapException {
        try {
            ruleManager = RuleManagerImpl.getInstance();
        } catch (DBConnectionFailureException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void testCRUDRules() {

        try {
            double random = Math.random();

            //test rule exists
            Assert.assertFalse(ruleManager.isRuleExists(ruleNameBase + String.valueOf((int)(random * 10))));

            //test create
            Rule rule = ruleManager.createRule(ruleNameBase + String.valueOf((int)(random * 10)), cpu, greaterThan,
                    (int)(random * 100), 1);
            Assert.assertNotNull(rule);

            //test rule exists
            Assert.assertTrue(ruleManager.isRuleExists(ruleNameBase + String.valueOf((int)(random * 10))));

            //test read
            Rule retrievedRule = ruleManager.getRule(ruleNameBase + String.valueOf((int)(random * 10)));
            checkRuleAttributes(retrievedRule, random);

            //test update
            rule.setComparator(RuleSupport.Comparator.LESS_THAN_OR_EQUAL);
            rule.setOperationAction(-1);
            ruleManager.updateRule(rule.getRuleName(), rule);
            Rule updatedRule = ruleManager.getRule(rule.getRuleName());
            Assert.assertEquals(RuleSupport.Comparator.LESS_THAN_OR_EQUAL, updatedRule.getComparator());
            Assert.assertEquals(-1, updatedRule.getOperationAction());

            //test rule usage
            boolean isRuleUsed = ruleManager.isRuleInUse(rule.getRuleName());
            Assert.assertFalse(isRuleUsed);

            String[] groupNames = ruleManager.getRuleUsage(rule.getRuleName());
            Assert.assertTrue(groupNames.length == 0);

            //test rule usage : when rule is in use : tested in GroupTest

            //test delete
            ruleManager.deleteRule(rule.getRuleName());
            Rule afterDeletion = ruleManager.getRule(rule.getRuleName());
            Assert.assertNull(afterDeletion);

        } catch (HoneyTapException e) {
            throw new IllegalStateException(e);
        }
    }

    private void checkRuleAttributes(Rule retrievedRule, double random) {
        Assert.assertEquals(ruleNameBase + String.valueOf((int)(random * 10)), retrievedRule.getRuleName());
        Assert.assertEquals(cpu, retrievedRule.getResourceType());
        Assert.assertEquals(greaterThan, retrievedRule.getComparator());
    }
}
