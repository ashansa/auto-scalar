package se.kth.autoscalar.scaling.group;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import se.kth.autoscalar.common.monitoring.RuleSupport.Comparator;
import se.kth.autoscalar.common.monitoring.RuleSupport.ResourceType;
import se.kth.autoscalar.scaling.exceptions.DBConnectionFailureException;
import se.kth.autoscalar.scaling.exceptions.ElasticScalarException;
import se.kth.autoscalar.scaling.rules.Rule;
import se.kth.autoscalar.scaling.rules.RuleManager;
import se.kth.autoscalar.scaling.rules.RuleManagerImpl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.fail;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class GroupTest {

    private static GroupManager groupManager;
    private static RuleManager ruleManager;
    private String groupBaseName = "my_group";
    private String ruleNameBase = "my_rule";
    private int coolingTimeOut = 60;
    private int coolingTimeIn = 300;

    @BeforeClass
    public static void init() throws ElasticScalarException {
        groupManager = GroupManagerImpl.getInstance();
        ruleManager = RuleManagerImpl.getInstance();
    }

    @Test
    public void testCRUDGroup() throws ElasticScalarException {
        double random = Math.random();
        String groupName = groupBaseName + String.valueOf((int)(random * 10));

        //test group exists
        Assert.assertFalse(groupManager.isGroupExists(groupName));

        // test create group with non existant rule
        try {
            groupManager.createGroup( groupName, (int)(random * 10),
                    (int)(random * 100), coolingTimeOut, coolingTimeIn, new String[]{"wrongRule"});

            fail("Expected exception not thrown");

        } catch (Exception e) {
            Assert.assertEquals(ElasticScalarException.class, e.getClass());
        }

        //test create group
        Rule rule = ruleManager.createRule(ruleNameBase + String.valueOf((int)(random * 10)),
                ResourceType.CPU_PERCENTAGE, Comparator.GREATER_THAN, (int)(random * 100), 1);
        Assert.assertNotNull(rule);

        Group group = groupManager.createGroup(groupName, (int)(random * 10),
                (int)(random * 100), coolingTimeOut, coolingTimeIn, new String[]{rule.getRuleName()});
        Assert.assertNotNull(group);

        //test exists
        Assert.assertTrue(groupManager.isGroupExists(groupName));

        //test get
        Group retrievedGroup = groupManager.getGroup(groupName);
        Assert.assertNotNull(retrievedGroup);

        //test update
        Group groupToUpdate = retrievedGroup;
        groupToUpdate.setCoolingTimeUp(coolingTimeOut + 10);
        groupManager.updateGroup(groupName, groupToUpdate);

        Group updated = groupManager.getGroup(groupName);
        Assert.assertEquals(groupToUpdate.getCoolingTimeUp(), updated.getCoolingTimeUp());

        //test add rule
        Rule rule2 = ruleManager.createRule(ruleNameBase + String.valueOf((int)(random * 10) + 1),
                ResourceType.CPU_PERCENTAGE, Comparator.GREATER_THAN, (int)(random * 100), 1);
        ArrayList<String> rulesOfGroup = new ArrayList<String>(Arrays.asList(groupManager.getGroup(groupName).getRuleNames()));
        Assert.assertFalse(rulesOfGroup.contains(rule2.getRuleName()));

        groupManager.addRuleToGroup(groupName, rule2.getRuleName());
        ArrayList<String> newRulesOfGroup = new ArrayList<String>(Arrays.asList(groupManager.getGroup(groupName).getRuleNames()));
        Assert.assertTrue(newRulesOfGroup.contains(rule2.getRuleName()));

        //test rule usages
        String[] groups = ruleManager.getRuleUsage(rule2.getRuleName());
        Assert.assertTrue(groups.length > 0);

        //test remove rule
        groupManager.removeRuleFromGroup(groupName, rule2.getRuleName());
        ArrayList<String> ruleRemovedGroup = new ArrayList<String>(Arrays.asList(groupManager.getGroup(groupName).getRuleNames()));
        Assert.assertFalse(ruleRemovedGroup.contains(rule2.getRuleName()));

        String[] groups2 = ruleManager.getRuleUsage(rule2.getRuleName());
        Assert.assertFalse(groups2.length > 0);

        //test delete
        groupManager.deleteGroup(groupName);
        Assert.assertFalse(groupManager.isGroupExists(groupName));
        ruleManager.deleteRule(rule.getRuleName());
        ruleManager.deleteRule(rule2.getRuleName());
    }

    @AfterClass
    public static void cleanUp() throws DBConnectionFailureException, SQLException {
        /*System.out.println("====== cleaning up : GroupTest =========");
        PreparedStatement statement1 = DBUtil.getDBConnection().prepareStatement("drop table Group_Rule ");
        statement1.executeUpdate();
        statement1.close();

        PreparedStatement statement2 = DBUtil.getDBConnection().prepareStatement("drop table Rule ");
        statement2.executeUpdate();
        statement2.close();

        PreparedStatement statement3 = DBUtil.getDBConnection().prepareStatement("drop table ScaleGroup ");
        statement3.executeUpdate();
        statement3.close();

        System.out.println("====== cleaned up : GroupTest =========");*/

    }
}