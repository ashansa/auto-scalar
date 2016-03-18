package se.kth.autoscalar.scaling.group;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import se.kth.autoscalar.common.monitoring.RuleSupport.Comparator;
import se.kth.autoscalar.common.monitoring.RuleSupport.ResourceType;
import se.kth.autoscalar.scaling.core.ElasticScalarAPI;
import se.kth.autoscalar.scaling.exceptions.DBConnectionFailureException;
import se.kth.autoscalar.scaling.exceptions.ElasticScalarException;
import se.kth.autoscalar.scaling.rules.Rule;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class GroupTest {

    private static ElasticScalarAPI elasticScalarAPI;
//    private static GroupManager groupManager;
//    private static RuleManager ruleManager;
    private String groupBaseName = "my_group";
    private String ruleNameBase = "my_rule";
    private int coolingTimeOut = 60;
    private int coolingTimeIn = 300;

    @BeforeClass
    public static void init() throws ElasticScalarException {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
        elasticScalarAPI = new ElasticScalarAPI();
//        groupManager = GroupManagerImpl.getInstance();
//        ruleManager = RuleManagerImpl.getInstance();
    }

    @Test
    public void testCRUDGroup() throws ElasticScalarException {
        double random = Math.random();
        String groupName = groupBaseName + String.valueOf((int)(random * 10));

        Map<Group.ResourceRequirement, Integer> minReq = new HashMap<Group.ResourceRequirement, Integer>();
        minReq.put(Group.ResourceRequirement.NUMBER_OF_VCPUS, 4);
        minReq.put(Group.ResourceRequirement.RAM, 8);
        minReq.put(Group.ResourceRequirement.STORAGE, 50);

        //test group exists
        Assert.assertFalse(elasticScalarAPI.isGroupExists(groupName));

        // test create group with non existant rule
        try {

            elasticScalarAPI.createGroup( groupName, (int)(random * 10),
                    (int)(random * 100), coolingTimeOut, coolingTimeIn, new String[]{"wrongRule"}, minReq, 2.0f);

            fail("Expected exception not thrown");

        } catch (Exception e) {
            Assert.assertEquals(ElasticScalarException.class, e.getClass());
        }

        //test create group
        Rule rule = elasticScalarAPI.createRule(ruleNameBase + String.valueOf((int)(random * 10)),
                ResourceType.CPU_PERCENTAGE, Comparator.GREATER_THAN, (int)(random * 100), 1);
        Assert.assertNotNull(rule);

        Group group = elasticScalarAPI.createGroup(groupName, (int)(random * 10),
                (int)(random * 100), coolingTimeOut, coolingTimeIn, new String[]{rule.getRuleName()}, minReq, 2.0f);
        Assert.assertNotNull(group);

        //test exists
        Assert.assertTrue(elasticScalarAPI.isGroupExists(groupName));

        //test get
        Group retrievedGroup = elasticScalarAPI.getGroup(groupName);
        Assert.assertNotNull(retrievedGroup);

        //test update
        Group groupToUpdate = retrievedGroup;
        groupToUpdate.setCoolingTimeUp(coolingTimeOut + 10);
        elasticScalarAPI.updateGroup(groupName, groupToUpdate);

        Group updated = elasticScalarAPI.getGroup(groupName);
        Assert.assertEquals(groupToUpdate.getCoolingTimeUp(), updated.getCoolingTimeUp());

        //test add rule
        Rule rule2 = elasticScalarAPI.createRule(ruleNameBase + String.valueOf((int)(random * 10) + 1),
                ResourceType.CPU_PERCENTAGE, Comparator.GREATER_THAN, (int)(random * 100), 1);
        ArrayList<String> rulesOfGroup = new ArrayList<String>(Arrays.asList(elasticScalarAPI.getGroup(groupName).getRuleNames()));
        Assert.assertFalse(rulesOfGroup.contains(rule2.getRuleName()));

        elasticScalarAPI.addRuleToGroup(groupName, rule2.getRuleName());
        ArrayList<String> newRulesOfGroup = new ArrayList<String>(Arrays.asList(elasticScalarAPI.getGroup(groupName).getRuleNames()));
        Assert.assertTrue(newRulesOfGroup.contains(rule2.getRuleName()));

        //test rule usages
        String[] groups = elasticScalarAPI.getRuleUsage(rule2.getRuleName());
        Assert.assertTrue(groups.length > 0);

        //test remove rule
        elasticScalarAPI.removeRuleFromGroup(groupName, rule2.getRuleName());
        ArrayList<String> ruleRemovedGroup = new ArrayList<String>(Arrays.asList(elasticScalarAPI.getGroup(groupName).getRuleNames()));
        Assert.assertFalse(ruleRemovedGroup.contains(rule2.getRuleName()));

        String[] groups2 = elasticScalarAPI.getRuleUsage(rule2.getRuleName());
        Assert.assertFalse(groups2.length > 0);

        //test delete
        elasticScalarAPI.deleteGroup(groupName);
        Assert.assertFalse(elasticScalarAPI.isGroupExists(groupName));
        elasticScalarAPI.deleteRule(rule.getRuleName());
        elasticScalarAPI.deleteRule(rule2.getRuleName());
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
