package se.kth.autoscalar.scaling;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import se.kth.autoscalar.scaling.core.AutoScalarAPI;
import se.kth.autoscalar.scaling.exceptions.AutoScalarException;
import se.kth.autoscalar.scaling.group.Group;
import se.kth.autoscalar.scaling.monitoring.RuleSupport;
import se.kth.autoscalar.scaling.profile.ProfiledResourceEvent;
import se.kth.autoscalar.scaling.rules.Rule;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class AutoScalingTest {

    private static AutoScalarAPI autoScalarAPI;

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
    public static void init() throws AutoScalarException {
        autoScalarAPI = AutoScalarAPI.getInstance();
    }

    @Test
    public void testNoOfMachineChanges() throws AutoScalarException {
        setBasicRulesNGroup();

        //test for scale out
        ProfiledResourceEvent profiledResourceEvent1 = new ProfiledResourceEvent(groupId);
        profiledResourceEvent1.addResourceThresholds(RuleSupport.ResourceType.CPU_PERCENTAGE.name().concat(
                Constants.SEPARATOR).concat(RuleSupport.Comparator.GREATER_THAN_OR_EQUAL.name()), (float) (random * 100 + 5)); //aligned with +1
        profiledResourceEvent1.addResourceThresholds(RuleSupport.ResourceType.RAM_PERCENTAGE.name().concat(
                Constants.SEPARATOR).concat(RuleSupport.Comparator.GREATER_THAN_OR_EQUAL.name()), (float) (random * 100));  // aligned with no changes
        Assert.assertEquals(1, autoScalarAPI.getNumberOfMachineChanges(profiledResourceEvent1));

        profiledResourceEvent1.addResourceThresholds(RuleSupport.ResourceType.RAM_PERCENTAGE.name().concat(
                Constants.SEPARATOR).concat(RuleSupport.Comparator.GREATER_THAN_OR_EQUAL.name()), (float) (random * 100 + 6));  // aligned with no changes
        Assert.assertEquals(2, autoScalarAPI.getNumberOfMachineChanges(profiledResourceEvent1));

        Rule ramLess =  autoScalarAPI.createRule(RULE_BASE_NAME + String.valueOf((int)(random * 10) + 2),
                RuleSupport.ResourceType.RAM_PERCENTAGE, RuleSupport.Comparator.LESS_THAN, (float) ((random * 10) + 30.5f) , -1);
        autoScalarAPI.addRuleToGroup(groupId, ramLess.getRuleName());
        profiledResourceEvent1.addResourceThresholds(RuleSupport.ResourceType.RAM_PERCENTAGE.name().concat(
                Constants.SEPARATOR).concat(RuleSupport.Comparator.LESS_THAN_OR_EQUAL.name()), (float) (random * 10)); //aligned with -1
        Assert.assertEquals(2, autoScalarAPI.getNumberOfMachineChanges(profiledResourceEvent1));   //since there are +values, should not scale down

        //test for scale in
        ProfiledResourceEvent profiledResourceEvent2 = new ProfiledResourceEvent(groupId);
        profiledResourceEvent2.addResourceThresholds(RuleSupport.ResourceType.RAM_PERCENTAGE.name().concat(
                Constants.SEPARATOR).concat(RuleSupport.Comparator.LESS_THAN_OR_EQUAL.name()), (float) (random * 10)); //aligned with -1
        Assert.assertEquals(-1, autoScalarAPI.getNumberOfMachineChanges(profiledResourceEvent2));

        Rule cpuLess =  autoScalarAPI.createRule(RULE_BASE_NAME + String.valueOf((int)(random * 10) + 3),
                RuleSupport.ResourceType.CPU_PERCENTAGE, RuleSupport.Comparator.LESS_THAN_OR_EQUAL, (float) ((random * 10) + 20.5f) , -2);
        autoScalarAPI.addRuleToGroup(groupId, cpuLess.getRuleName());
        profiledResourceEvent2.addResourceThresholds(RuleSupport.ResourceType.CPU_PERCENTAGE.name().concat(
                Constants.SEPARATOR).concat(RuleSupport.Comparator.LESS_THAN_OR_EQUAL.name()), (float) (random * 10)); //aligned with -2
        Assert.assertEquals(-2, autoScalarAPI.getNumberOfMachineChanges(profiledResourceEvent2));   //should get -2 since both -1,-2 rules matches

        autoScalarAPI.deleteGroup(cpuLess.getRuleName());
        autoScalarAPI.deleteRule(ramLess.getRuleName());
    }

    private void setBasicRulesNGroup() {

        try {
            random = Math.random();
            groupId = GROUP_BASE_NAME + String.valueOf((int) (random * 10));
            rule1 = autoScalarAPI.createRule(RULE_BASE_NAME + String.valueOf((int) (random * 10)),
                    RuleSupport.ResourceType.CPU_PERCENTAGE, RuleSupport.Comparator.GREATER_THAN, (float) (random * 100), 1);
            rule2 = autoScalarAPI.createRule(RULE_BASE_NAME + String.valueOf((int)(random * 10) + 1),
                    RuleSupport.ResourceType.RAM_PERCENTAGE, RuleSupport.Comparator.GREATER_THAN_OR_EQUAL, (float) ((random * 100) + 2) , 2);

            Map<Group.ResourceRequirement, Integer> minReq = new HashMap<Group.ResourceRequirement, Integer>();
            minReq.put(Group.ResourceRequirement.NUMBER_OF_VCPUS, 4);
            minReq.put(Group.ResourceRequirement.RAM, 8);
            minReq.put(Group.ResourceRequirement.STORAGE, 50);

            group = autoScalarAPI.createGroup(groupId, (int)(random * 10), (int)(random * 100), coolingTimeOut,
                    coolingTimeIn, new String[]{rule1.getRuleName(), rule2.getRuleName()}, minReq, 2.0f);

        } catch (AutoScalarException e) {
            throw new IllegalStateException(e);
        }
    }

    @AfterClass
    public static void cleanup() throws AutoScalarException {
        autoScalarAPI.deleteRule(rule1.getRuleName());
        autoScalarAPI.deleteRule(rule2.getRuleName());
        autoScalarAPI.deleteGroup(group.getGroupName());
    }
}
