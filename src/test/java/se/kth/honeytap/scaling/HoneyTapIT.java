package se.kth.honeytap.scaling;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import se.kth.honeytap.scaling.core.HoneyTapAPI;
import se.kth.honeytap.scaling.exceptions.HoneyTapException;
import se.kth.honeytap.scaling.group.Group;
import se.kth.honeytap.scaling.monitoring.RuleSupport;
import se.kth.honeytap.scaling.profile.ProfiledResourceEvent;
import se.kth.honeytap.scaling.rules.Rule;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class HoneyTapIT {

    private static HoneyTapAPI honeyTapAPI;

    private String GROUP_BASE_NAME = "my_group";
    private String RULE_BASE_NAME = "my_rule";
    private int coolingTimeOut = 0;
    private int coolingTimeIn = 0;

    double random;
    static Rule rule1;
    static Rule rule2;
    static Group group;
    String groupId;
    String vmId = "vm1";

    @BeforeClass
    public static void init() throws HoneyTapException {
        honeyTapAPI = new HoneyTapAPI();
    }

    @Test
    public void testNoOfMachineChanges() throws HoneyTapException {
        setBasicRulesNGroup();

        //test for scale out
        ProfiledResourceEvent profiledResourceEvent1 = new ProfiledResourceEvent(groupId);
        profiledResourceEvent1.addResourceThresholds(RuleSupport.ResourceType.CPU.name().concat(
                Constants.SEPARATOR).concat(RuleSupport.Comparator.GREATER_THAN_OR_EQUAL.name()), (float) (random * 100 + 5)); //aligned with +1
        profiledResourceEvent1.addResourceThresholds(RuleSupport.ResourceType.RAM.name().concat(
                Constants.SEPARATOR).concat(RuleSupport.Comparator.GREATER_THAN_OR_EQUAL.name()), (float) (random * 100));  // aligned with no changes
        Assert.assertEquals(1, honeyTapAPI.getNumberOfMachineChanges(profiledResourceEvent1));

        profiledResourceEvent1.addResourceThresholds(RuleSupport.ResourceType.RAM.name().concat(
                Constants.SEPARATOR).concat(RuleSupport.Comparator.GREATER_THAN_OR_EQUAL.name()), (float) (random * 100 + 6));  // aligned with no changes
        Assert.assertEquals(2, honeyTapAPI.getNumberOfMachineChanges(profiledResourceEvent1));

        Rule ramLess =  honeyTapAPI.createRule(RULE_BASE_NAME + String.valueOf((int)(random * 10) + 2),
                RuleSupport.ResourceType.RAM, RuleSupport.Comparator.LESS_THAN, (float) ((random * 10) + 30.5f) , -1);
        honeyTapAPI.addRuleToGroup(groupId, ramLess.getRuleName());
        profiledResourceEvent1.addResourceThresholds(RuleSupport.ResourceType.RAM.name().concat(
                Constants.SEPARATOR).concat(RuleSupport.Comparator.LESS_THAN_OR_EQUAL.name()), (float) (random * 10)); //aligned with -1
        Assert.assertEquals(2, honeyTapAPI.getNumberOfMachineChanges(profiledResourceEvent1));   //since there are +values, should not scale down

        //test for scale in
        ProfiledResourceEvent profiledResourceEvent2 = new ProfiledResourceEvent(groupId);
        profiledResourceEvent2.addResourceThresholds(RuleSupport.ResourceType.RAM.name().concat(
                Constants.SEPARATOR).concat(RuleSupport.Comparator.LESS_THAN_OR_EQUAL.name()), (float) (random * 10)); //aligned with -1
        Assert.assertEquals(-1, honeyTapAPI.getNumberOfMachineChanges(profiledResourceEvent2));

        Rule cpuLess =  honeyTapAPI.createRule(RULE_BASE_NAME + String.valueOf((int)(random * 10) + 3),
                RuleSupport.ResourceType.CPU, RuleSupport.Comparator.LESS_THAN_OR_EQUAL, (float) ((random * 10) + 20.5f) , -2);
        honeyTapAPI.addRuleToGroup(groupId, cpuLess.getRuleName());
        profiledResourceEvent2.addResourceThresholds(RuleSupport.ResourceType.CPU.name().concat(
                Constants.SEPARATOR).concat(RuleSupport.Comparator.LESS_THAN_OR_EQUAL.name()), (float) (random * 10)); //aligned with -2
        Assert.assertEquals(-2, honeyTapAPI.getNumberOfMachineChanges(profiledResourceEvent2));   //should get -2 since both -1,-2 rules matches

        honeyTapAPI.deleteRule(cpuLess.getRuleName());
        honeyTapAPI.deleteRule(ramLess.getRuleName());
    }

    private void setBasicRulesNGroup() {

        try {
            random = Math.random();
            groupId = GROUP_BASE_NAME + String.valueOf((int) (random * 10));
            rule1 = honeyTapAPI.createRule(RULE_BASE_NAME + String.valueOf((int) (random * 10)),
                    RuleSupport.ResourceType.CPU, RuleSupport.Comparator.GREATER_THAN, (float) (random * 100), 1);
            rule2 = honeyTapAPI.createRule(RULE_BASE_NAME + String.valueOf((int)(random * 10) + 1),
                    RuleSupport.ResourceType.RAM, RuleSupport.Comparator.GREATER_THAN_OR_EQUAL, (float) ((random * 100) + 2) , 2);

            Map<Group.ResourceRequirement, Integer> minReq = new HashMap<Group.ResourceRequirement, Integer>();
            minReq.put(Group.ResourceRequirement.NUMBER_OF_VCPUS, 4);
            minReq.put(Group.ResourceRequirement.RAM, 8);
            minReq.put(Group.ResourceRequirement.STORAGE, 50);

            group = honeyTapAPI.createGroup(groupId, (int)(random * 10), (int)(random * 100), coolingTimeOut,
                    coolingTimeIn, new String[]{rule1.getRuleName(), rule2.getRuleName()}, minReq, 2.0f);

        } catch (HoneyTapException e) {
            throw new IllegalStateException(e);
        }
    }

    @AfterClass
    public static void cleanup() throws HoneyTapException {
        honeyTapAPI.deleteRule(rule1.getRuleName());
        honeyTapAPI.deleteRule(rule2.getRuleName());
        honeyTapAPI.deleteGroup(group.getGroupName());
    }
}
