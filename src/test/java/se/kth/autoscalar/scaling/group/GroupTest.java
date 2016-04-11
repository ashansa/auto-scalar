package se.kth.autoscalar.scaling.group;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import se.kth.autoscalar.scaling.monitoring.RuleSupport.Comparator;
import se.kth.autoscalar.scaling.monitoring.RuleSupport.ResourceType;
import se.kth.autoscalar.scaling.core.AutoScalarAPI;
import se.kth.autoscalar.scaling.exceptions.AutoScalarException;
import se.kth.autoscalar.scaling.rules.Rule;

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

    private static AutoScalarAPI autoScalarAPI;
    private String groupBaseName = "my_group";
    private String ruleNameBase = "my_rule";
    private int coolingTimeOut = 60;
    private int coolingTimeIn = 300;

    @BeforeClass
    public static void init() throws AutoScalarException {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
        autoScalarAPI = AutoScalarAPI.getInstance();
    }

    @Test
    public void testCRUDGroup() throws AutoScalarException {
        double random = Math.random();
        String groupName = groupBaseName + String.valueOf((int)(random * 10));

        Map<Group.ResourceRequirement, Integer> minReq = new HashMap<Group.ResourceRequirement, Integer>();
        minReq.put(Group.ResourceRequirement.NUMBER_OF_VCPUS, 4);
        minReq.put(Group.ResourceRequirement.RAM, 8);
        minReq.put(Group.ResourceRequirement.STORAGE, 50);

        //test group exists
        Assert.assertFalse(autoScalarAPI.isGroupExists(groupName));

        // test create group with non existant rule
        try {

            autoScalarAPI.createGroup( groupName, (int)(random * 10),
                    (int)(random * 100), coolingTimeOut, coolingTimeIn, new String[]{"wrongRule"}, minReq, 2.0f);

            fail("Expected exception not thrown");

        } catch (Exception e) {
            Assert.assertEquals(AutoScalarException.class, e.getClass());
        }

        //test create group
        Rule rule = autoScalarAPI.createRule(ruleNameBase + String.valueOf((int)(random * 10)),
                ResourceType.CPU_PERCENTAGE, Comparator.GREATER_THAN, (int)(random * 100), 1);
        Assert.assertNotNull(rule);

        Group group = autoScalarAPI.createGroup(groupName, (int)(random * 10),
                (int)(random * 100), coolingTimeOut, coolingTimeIn, new String[]{rule.getRuleName()}, minReq, 2.0f);
        Assert.assertNotNull(group);

        //test exists
        Assert.assertTrue(autoScalarAPI.isGroupExists(groupName));

        //test get
        Group retrievedGroup = autoScalarAPI.getGroup(groupName);
        Assert.assertNotNull(retrievedGroup);

        //test update
        Group groupToUpdate = retrievedGroup;
        groupToUpdate.setCoolingTimeOut(coolingTimeOut + 10);
        autoScalarAPI.updateGroup(groupName, groupToUpdate);

        Group updated = autoScalarAPI.getGroup(groupName);
        Assert.assertEquals(groupToUpdate.getCoolingTimeOut(), updated.getCoolingTimeOut());

        //test add rule
        Rule rule2 = autoScalarAPI.createRule(ruleNameBase + String.valueOf((int)(random * 10) + 1),
                ResourceType.CPU_PERCENTAGE, Comparator.GREATER_THAN, (int)(random * 100), 1);
        ArrayList<String> rulesOfGroup = new ArrayList<String>(Arrays.asList(autoScalarAPI.getGroup(groupName).getRuleNames()));
        Assert.assertFalse(rulesOfGroup.contains(rule2.getRuleName()));

        autoScalarAPI.addRuleToGroup(groupName, rule2.getRuleName());
        ArrayList<String> newRulesOfGroup = new ArrayList<String>(Arrays.asList(autoScalarAPI.getGroup(groupName).getRuleNames()));
        Assert.assertTrue(newRulesOfGroup.contains(rule2.getRuleName()));

        //test rule usages
        String[] groups = autoScalarAPI.getRuleUsage(rule2.getRuleName());
        Assert.assertTrue(groups.length > 0);

        //test remove rule
        autoScalarAPI.removeRuleFromGroup(groupName, rule2.getRuleName());
        ArrayList<String> ruleRemovedGroup = new ArrayList<String>(Arrays.asList(autoScalarAPI.getGroup(groupName).getRuleNames()));
        Assert.assertFalse(ruleRemovedGroup.contains(rule2.getRuleName()));

        String[] groups2 = autoScalarAPI.getRuleUsage(rule2.getRuleName());
        Assert.assertFalse(groups2.length > 0);

        //test delete
        autoScalarAPI.deleteGroup(groupName);
        Assert.assertFalse(autoScalarAPI.isGroupExists(groupName));
        autoScalarAPI.deleteRule(rule.getRuleName());
        autoScalarAPI.deleteRule(rule2.getRuleName());
    }
}
