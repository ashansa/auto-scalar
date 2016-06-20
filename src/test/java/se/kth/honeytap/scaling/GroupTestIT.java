package se.kth.honeytap.scaling;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import se.kth.honeytap.scaling.core.HoneyTapAPI;
import se.kth.honeytap.scaling.group.Group;
import se.kth.honeytap.scaling.exceptions.HoneyTapException;
import se.kth.honeytap.scaling.rules.Rule;
import se.kth.honeytap.scaling.monitoring.RuleSupport;

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
public class GroupTestIT {

    private static HoneyTapAPI honeyTapAPI;
    private String groupBaseName = "my_group";
    private String ruleNameBase = "my_rule";
    private int coolingTimeOut = 60;
    private int coolingTimeIn = 300;

    @BeforeClass
    public static void init() throws HoneyTapException {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
        honeyTapAPI = new HoneyTapAPI();
    }

    @Test
    public void testCRUDGroup() throws HoneyTapException {
        double random = Math.random();
        String groupName = groupBaseName + String.valueOf((int)(random * 10));

        Map<Group.ResourceRequirement, Integer> minReq = new HashMap<Group.ResourceRequirement, Integer>();
        minReq.put(Group.ResourceRequirement.NUMBER_OF_VCPUS, 4);
        minReq.put(Group.ResourceRequirement.RAM, 8);
        minReq.put(Group.ResourceRequirement.STORAGE, 50);

        //test group exists
        Assert.assertFalse(honeyTapAPI.isGroupExists(groupName));

        // test create group with non existant rule
        try {

            honeyTapAPI.createGroup( groupName, (int)(random * 10),
                    (int)(random * 100), coolingTimeOut, coolingTimeIn, new String[]{"wrongRule"}, minReq, 2.0f);

            fail("Expected exception not thrown");

        } catch (Exception e) {
            Assert.assertEquals(HoneyTapException.class, e.getClass());
        }

        //test create group
        Rule rule = honeyTapAPI.createRule(ruleNameBase + String.valueOf((int)(random * 10)),
                RuleSupport.ResourceType.CPU, RuleSupport.Comparator.GREATER_THAN, (int)(random * 100), 1);
        Assert.assertNotNull(rule);

        Group group = honeyTapAPI.createGroup(groupName, (int)(random * 10),
                (int)(random * 100), coolingTimeOut, coolingTimeIn, new String[]{rule.getRuleName()}, minReq, 2.0f);
        Assert.assertNotNull(group);

        //test exists
        Assert.assertTrue(honeyTapAPI.isGroupExists(groupName));

        //test get
        Group retrievedGroup = honeyTapAPI.getGroup(groupName);
        Assert.assertNotNull(retrievedGroup);

        //test update
        Group groupToUpdate = retrievedGroup;
        groupToUpdate.setCoolingTimeOut(coolingTimeOut + 10);
        honeyTapAPI.updateGroup(groupName, groupToUpdate);

        Group updated = honeyTapAPI.getGroup(groupName);
        Assert.assertEquals(groupToUpdate.getCoolingTimeOut(), updated.getCoolingTimeOut());

        //test add rule
        Rule rule2 = honeyTapAPI.createRule(ruleNameBase + String.valueOf((int)(random * 10) + 1),
                RuleSupport.ResourceType.CPU, RuleSupport.Comparator.GREATER_THAN, (int)(random * 100), 1);
        ArrayList<String> rulesOfGroup = new ArrayList<String>(Arrays.asList(honeyTapAPI.getGroup(groupName).getRuleNames()));
        Assert.assertFalse(rulesOfGroup.contains(rule2.getRuleName()));

        honeyTapAPI.addRuleToGroup(groupName, rule2.getRuleName());
        ArrayList<String> newRulesOfGroup = new ArrayList<String>(Arrays.asList(honeyTapAPI.getGroup(groupName).getRuleNames()));
        Assert.assertTrue(newRulesOfGroup.contains(rule2.getRuleName()));

        //test rule usages
        String[] groups = honeyTapAPI.getRuleUsage(rule2.getRuleName());
        Assert.assertTrue(groups.length > 0);

        //test remove rule
        honeyTapAPI.removeRuleFromGroup(groupName, rule2.getRuleName());
        ArrayList<String> ruleRemovedGroup = new ArrayList<String>(Arrays.asList(honeyTapAPI.getGroup(groupName).getRuleNames()));
        Assert.assertFalse(ruleRemovedGroup.contains(rule2.getRuleName()));

        String[] groups2 = honeyTapAPI.getRuleUsage(rule2.getRuleName());
        Assert.assertFalse(groups2.length > 0);

        //test delete
        honeyTapAPI.deleteGroup(groupName);
        Assert.assertFalse(honeyTapAPI.isGroupExists(groupName));
        honeyTapAPI.deleteRule(rule.getRuleName());
        honeyTapAPI.deleteRule(rule2.getRuleName());
    }
}
