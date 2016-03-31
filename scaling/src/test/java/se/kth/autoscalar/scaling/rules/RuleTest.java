package se.kth.autoscalar.scaling.rules;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import se.kth.autoscalar.common.monitoring.RuleSupport.Comparator;
import se.kth.autoscalar.common.monitoring.RuleSupport.ResourceType;
import se.kth.autoscalar.scaling.exceptions.DBConnectionFailureException;
import se.kth.autoscalar.scaling.exceptions.AutoScalarException;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class RuleTest {

    private static final String ruleNameBase = "testRule";
    private static final ResourceType cpu = ResourceType.CPU_PERCENTAGE;
    private static final Comparator greaterThan = Comparator.GREATER_THAN;
    private static RuleManager ruleManager;

    @BeforeClass
    public static void setUp() throws AutoScalarException {
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
            rule.setComparator(Comparator.LESS_THAN_OR_EQUAL);
            rule.setOperationAction(-1);
            ruleManager.updateRule(rule.getRuleName(), rule);
            Rule updatedRule = ruleManager.getRule(rule.getRuleName());
            Assert.assertEquals(Comparator.LESS_THAN_OR_EQUAL, updatedRule.getComparator());
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

        } catch (AutoScalarException e) {
            throw new IllegalStateException(e);
        }
    }

    private void checkRuleAttributes(Rule retrievedRule, double random) {
        Assert.assertEquals(ruleNameBase + String.valueOf((int)(random * 10)), retrievedRule.getRuleName());
        Assert.assertEquals(cpu, retrievedRule.getResourceType());
        Assert.assertEquals(greaterThan, retrievedRule.getComparator());
    }
}
